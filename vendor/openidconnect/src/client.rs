use crate::{
    AccessToken, AdditionalClaims, AdditionalProviderMetadata, AuthDisplay, AuthPrompt, AuthType,
    AuthUrl, AuthenticationFlow, AuthorizationCode, AuthorizationRequest, ClaimName, ClaimType,
    ClientAuthMethod, ClientCredentialsTokenRequest, ClientId, ClientSecret, CodeTokenRequest,
    ConfigurationError, CsrfToken, DeviceAccessTokenRequest, DeviceAuthorizationRequest,
    DeviceAuthorizationResponse, DeviceAuthorizationUrl, EndpointMaybeSet, EndpointNotSet,
    EndpointSet, EndpointState, ErrorResponse, ExtraDeviceAuthorizationFields, GenderClaim,
    GrantType, IdTokenVerifier, IntrospectionRequest, IntrospectionUrl, IssuerUrl, JsonWebKey,
    JsonWebKeySet, JweContentEncryptionAlgorithm, JweKeyManagementAlgorithm, JwsSigningAlgorithm,
    Nonce, PasswordTokenRequest, ProviderMetadata, RedirectUrl, RefreshToken, RefreshTokenRequest,
    ResourceOwnerPassword, ResourceOwnerUsername, ResponseMode, ResponseType, RevocableToken,
    RevocationRequest, RevocationUrl, Scope, SubjectIdentifier, SubjectIdentifierType,
    TokenIntrospectionResponse, TokenResponse, TokenUrl, UserInfoRequest, UserInfoUrl,
};

use std::marker::PhantomData;

const OPENID_SCOPE: &str = "openid";

/// OpenID Connect client.
///
/// # Error Types
///
/// To enable compile time verification that only the correct and complete set of errors for the `Client` function being
/// invoked are exposed to the caller, the `Client` type is specialized on multiple implementations of the
/// [`ErrorResponse`] trait. The exact [`ErrorResponse`] implementation returned varies by the RFC that the invoked
/// `Client` function implements:
///
///   - Generic type `TE` (aka Token Error) for errors defined by [RFC 6749 OAuth 2.0 Authorization Framework](https://tools.ietf.org/html/rfc6749).
///   - Generic type `TRE` (aka Token Revocation Error) for errors defined by [RFC 7009 OAuth 2.0 Token Revocation](https://tools.ietf.org/html/rfc7009).
///
/// For example when revoking a token, error code `unsupported_token_type` (from RFC 7009) may be returned:
/// ```rust
/// # use http::status::StatusCode;
/// # use http::header::{HeaderValue, CONTENT_TYPE};
/// # use openidconnect::core::CoreClient;
/// # use openidconnect::{
/// #     AccessToken,
/// #     AuthUrl,
/// #     ClientId,
/// #     ClientSecret,
/// #     HttpResponse,
/// #     IssuerUrl,
/// #     JsonWebKeySet,
/// #     RequestTokenError,
/// #     RevocationErrorResponseType,
/// #     RevocationUrl,
/// #     TokenUrl,
/// # };
/// # use thiserror::Error;
/// #
/// # let client =
/// #     CoreClient::new(
/// #         ClientId::new("aaa".to_string()),
/// #         IssuerUrl::new("https://example".to_string()).unwrap(),
/// #         JsonWebKeySet::default(),
/// #     )
/// #     .set_client_secret(ClientSecret::new("bbb".to_string()))
/// #     .set_auth_uri(AuthUrl::new("https://example/authorize".to_string()).unwrap())
/// #     .set_token_uri(TokenUrl::new("https://example/token".to_string()).unwrap())
/// #     .set_revocation_url(RevocationUrl::new("https://revocation/url".to_string()).unwrap());
/// #
/// # #[derive(Debug, Error)]
/// # enum FakeError {
/// #     #[error("error")]
/// #     Err,
/// # }
/// #
/// # let http_client = |_| -> Result<HttpResponse, FakeError> {
/// #     Ok(http::Response::builder()
/// #         .status(StatusCode::BAD_REQUEST)
/// #         .header(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap())
/// #         .body(
/// #             r#"{"error": "unsupported_token_type",
/// #                 "error_description": "stuff happened",
/// #                 "error_uri": "https://errors"}"#
/// #             .to_string()
/// #             .into_bytes(),
/// #         )
/// #         .unwrap())
/// # };
/// #
/// let res = client
///     .revoke_token(AccessToken::new("some token".to_string()).into())
///     .unwrap()
///     .request(&http_client);
///
/// assert!(matches!(res, Err(
///     RequestTokenError::ServerResponse(err)) if matches!(err.error(),
///         RevocationErrorResponseType::UnsupportedTokenType)));
/// ```
#[derive(Clone, Debug)]
pub struct Client<
    AC,
    AD,
    GC,
    JE,
    K,
    P,
    TE,
    TR,
    TIR,
    RT,
    TRE,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
    HasUserInfoUrl,
> where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    oauth2_client: oauth2::Client<
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
    >,
    pub(crate) client_id: ClientId,
    client_secret: Option<ClientSecret>,
    pub(crate) issuer: IssuerUrl,
    userinfo_endpoint: Option<UserInfoUrl>,
    pub(crate) jwks: JsonWebKeySet<K>,
    id_token_signing_algs: Option<Vec<K::SigningAlgorithm>>,
    use_openid_scope: bool,
    _phantom: PhantomData<(AC, AD, GC, JE, P, HasUserInfoUrl)>,
}
impl<AC, AD, GC, JE, K, P, TE, TR, TIR, RT, TRE>
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        EndpointNotSet,
        EndpointNotSet,
        EndpointNotSet,
        EndpointNotSet,
        EndpointNotSet,
        EndpointNotSet,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
{
    /// Initialize an OpenID Connect client.
    pub fn new(client_id: ClientId, issuer: IssuerUrl, jwks: JsonWebKeySet<K>) -> Self {
        Client {
            oauth2_client: oauth2::Client::new(client_id.clone()),
            client_id,
            client_secret: None,
            issuer,
            userinfo_endpoint: None,
            jwks,
            id_token_signing_algs: None,
            use_openid_scope: true,
            _phantom: PhantomData,
        }
    }
}
impl<AC, AD, GC, JE, K, P, TE, TR, TIR, RT, TRE>
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        EndpointSet,
        EndpointNotSet,
        EndpointNotSet,
        EndpointNotSet,
        EndpointMaybeSet,
        EndpointMaybeSet,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
{
    /// Initialize an OpenID Connect client from OpenID Connect Discovery provider metadata.
    ///
    /// Use [`ProviderMetadata::discover`] or
    /// [`ProviderMetadata::discover_async`] to fetch the provider metadata.
    pub fn from_provider_metadata<A, CA, CN, CT, G, JK, RM, RS, S>(
        provider_metadata: ProviderMetadata<A, AD, CA, CN, CT, G, JE, JK, K, RM, RS, S>,
        client_id: ClientId,
        client_secret: Option<ClientSecret>,
    ) -> Self
    where
        A: AdditionalProviderMetadata,
        CA: ClientAuthMethod,
        CN: ClaimName,
        CT: ClaimType,
        G: GrantType,
        JK: JweKeyManagementAlgorithm,
        RM: ResponseMode,
        RS: ResponseType,
        S: SubjectIdentifierType,
    {
        let mut oauth2_client = oauth2::Client::new(client_id.clone())
            .set_auth_uri(provider_metadata.authorization_endpoint().clone())
            .set_token_uri_option(provider_metadata.token_endpoint().cloned());
        if let Some(ref client_secret) = client_secret {
            oauth2_client = oauth2_client.set_client_secret(client_secret.to_owned());
        }

        Client {
            oauth2_client,
            client_id,
            client_secret,
            issuer: provider_metadata.issuer().clone(),
            userinfo_endpoint: provider_metadata.userinfo_endpoint().cloned(),
            jwks: provider_metadata.jwks().to_owned(),
            id_token_signing_algs: Some(
                provider_metadata
                    .id_token_signing_alg_values_supported()
                    .to_owned(),
            ),
            use_openid_scope: true,
            _phantom: PhantomData,
        }
    }
}
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Set the type of client authentication used for communicating with the authorization
    /// server.
    ///
    /// The default is to use HTTP Basic authentication, as recommended in
    /// [Section 2.3.1 of RFC 6749](https://tools.ietf.org/html/rfc6749#section-2.3.1). Note that
    /// if a client secret is omitted (i.e., [`set_client_secret()`](Self::set_client_secret) is not
    /// called), [`AuthType::RequestBody`] is used regardless of the `auth_type` passed to
    /// this function.
    pub fn set_auth_type(mut self, auth_type: AuthType) -> Self {
        self.oauth2_client = self.oauth2_client.set_auth_type(auth_type);
        self
    }

    /// Return the type of client authentication used for communicating with the authorization
    /// server.
    pub fn auth_type(&self) -> &AuthType {
        self.oauth2_client.auth_type()
    }

    /// Set the authorization endpoint.
    ///
    /// The client uses the authorization endpoint to obtain authorization from the resource owner
    /// via user-agent redirection. This URL is used in all standard OAuth2 flows except the
    /// [Resource Owner Password Credentials Grant](https://tools.ietf.org/html/rfc6749#section-4.3)
    /// and the [Client Credentials Grant](https://tools.ietf.org/html/rfc6749#section-4.4).
    pub fn set_auth_uri(
        self,
        auth_uri: AuthUrl,
    ) -> Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        EndpointSet,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    > {
        Client {
            oauth2_client: self.oauth2_client.set_auth_uri(auth_uri),
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            userinfo_endpoint: self.userinfo_endpoint,
            jwks: self.jwks,
            id_token_signing_algs: self.id_token_signing_algs,
            use_openid_scope: self.use_openid_scope,
            _phantom: PhantomData,
        }
    }

    /// Return the Client ID.
    pub fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    /// Set the client secret.
    ///
    /// A client secret is generally used for confidential (i.e., server-side) OAuth2 clients and
    /// omitted from public (browser or native app) OAuth2 clients (see
    /// [RFC 8252](https://tools.ietf.org/html/rfc8252)).
    pub fn set_client_secret(mut self, client_secret: ClientSecret) -> Self {
        self.oauth2_client = self.oauth2_client.set_client_secret(client_secret.clone());
        self.client_secret = Some(client_secret);

        self
    }

    /// Set the [RFC 8628](https://tools.ietf.org/html/rfc8628) device authorization endpoint used
    /// for the Device Authorization Flow.
    ///
    /// See [`exchange_device_code()`](Self::exchange_device_code).
    pub fn set_device_authorization_url(
        self,
        device_authorization_url: DeviceAuthorizationUrl,
    ) -> Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        EndpointSet,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    > {
        Client {
            oauth2_client: self
                .oauth2_client
                .set_device_authorization_url(device_authorization_url),
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            userinfo_endpoint: self.userinfo_endpoint,
            jwks: self.jwks,
            id_token_signing_algs: self.id_token_signing_algs,
            use_openid_scope: self.use_openid_scope,
            _phantom: PhantomData,
        }
    }

    /// Set the [RFC 7662](https://tools.ietf.org/html/rfc7662) introspection endpoint.
    ///
    /// See [`introspect()`](Self::introspect).
    pub fn set_introspection_url(
        self,
        introspection_url: IntrospectionUrl,
    ) -> Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        EndpointSet,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    > {
        Client {
            oauth2_client: self.oauth2_client.set_introspection_url(introspection_url),
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            userinfo_endpoint: self.userinfo_endpoint,
            jwks: self.jwks,
            id_token_signing_algs: self.id_token_signing_algs,
            use_openid_scope: self.use_openid_scope,
            _phantom: PhantomData,
        }
    }

    /// Set the redirect URL used by the authorization endpoint.
    pub fn set_redirect_uri(mut self, redirect_url: RedirectUrl) -> Self {
        self.oauth2_client = self.oauth2_client.set_redirect_uri(redirect_url);
        self
    }

    /// Return the redirect URL used by the authorization endpoint.
    pub fn redirect_uri(&self) -> Option<&RedirectUrl> {
        self.oauth2_client.redirect_uri()
    }

    /// Set the [RFC 7009](https://tools.ietf.org/html/rfc7009) revocation endpoint.
    ///
    /// See [`revoke_token()`](Self::revoke_token).
    pub fn set_revocation_url(
        self,
        revocation_uri: RevocationUrl,
    ) -> Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        EndpointSet,
        HasTokenUrl,
        HasUserInfoUrl,
    > {
        Client {
            oauth2_client: self.oauth2_client.set_revocation_url(revocation_uri),
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            userinfo_endpoint: self.userinfo_endpoint,
            jwks: self.jwks,
            id_token_signing_algs: self.id_token_signing_algs,
            use_openid_scope: self.use_openid_scope,
            _phantom: PhantomData,
        }
    }

    /// Set the token endpoint.
    ///
    /// The client uses the token endpoint to exchange an authorization code for an access token,
    /// typically with client authentication. This URL is used in
    /// all standard OAuth2 flows except the
    /// [Implicit Grant](https://tools.ietf.org/html/rfc6749#section-4.2).
    pub fn set_token_uri(
        self,
        token_uri: TokenUrl,
    ) -> Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        EndpointSet,
        HasUserInfoUrl,
    > {
        Client {
            oauth2_client: self.oauth2_client.set_token_uri(token_uri),
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            userinfo_endpoint: self.userinfo_endpoint,
            jwks: self.jwks,
            id_token_signing_algs: self.id_token_signing_algs,
            use_openid_scope: self.use_openid_scope,
            _phantom: PhantomData,
        }
    }

    /// Set the user info endpoint.
    ///
    /// See [`user_info()`](Self::user_info).
    pub fn set_user_info_url(
        self,
        userinfo_endpoint: UserInfoUrl,
    ) -> Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        EndpointSet,
    > {
        Client {
            oauth2_client: self.oauth2_client,
            client_id: self.client_id,
            client_secret: self.client_secret,
            issuer: self.issuer,
            userinfo_endpoint: Some(userinfo_endpoint),
            jwks: self.jwks,
            id_token_signing_algs: self.id_token_signing_algs,
            use_openid_scope: self.use_openid_scope,
            _phantom: PhantomData,
        }
    }

    /// Enable the `openid` scope to be requested automatically.
    ///
    /// This scope is requested by default, so this function is only useful after previous calls to
    /// [`disable_openid_scope`][Client::disable_openid_scope].
    pub fn enable_openid_scope(mut self) -> Self {
        self.use_openid_scope = true;
        self
    }

    /// Disable the `openid` scope from being requested automatically.
    pub fn disable_openid_scope(mut self) -> Self {
        self.use_openid_scope = false;
        self
    }

    /// Return an ID token verifier for use with the [`IdToken::claims`](crate::IdToken::claims)
    /// method.
    pub fn id_token_verifier(&self) -> IdTokenVerifier<'_, K> {
        let verifier = if let Some(ref client_secret) = self.client_secret {
            IdTokenVerifier::new_confidential_client(
                self.client_id.clone(),
                client_secret.clone(),
                self.issuer.clone(),
                self.jwks.clone(),
            )
        } else {
            IdTokenVerifier::new_public_client(
                self.client_id.clone(),
                self.issuer.clone(),
                self.jwks.clone(),
            )
        };

        if let Some(id_token_signing_algs) = self.id_token_signing_algs.clone() {
            verifier.set_allowed_algs(id_token_signing_algs)
        } else {
            verifier
        }
    }
}

/// Methods requiring an authorization endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        EndpointSet,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Return the authorization endpoint.
    pub fn auth_uri(&self) -> &AuthUrl {
        self.oauth2_client.auth_uri()
    }

    /// Generate an authorization URL for a new authorization request.
    ///
    /// Requires [`set_auth_uri()`](Self::set_auth_uri) to have been previously
    /// called to set the authorization endpoint.
    ///
    /// NOTE: [Passing authorization request parameters as a JSON Web Token
    /// ](https://openid.net/specs/openid-connect-core-1_0.html#JWTRequests)
    /// instead of URL query parameters is not currently supported. The
    /// [`claims` parameter](https://openid.net/specs/openid-connect-core-1_0.html#ClaimsParameter)
    /// is also not directly supported, although the [`AuthorizationRequest::add_extra_param`]
    /// method can be used to add custom parameters, including `claims`.
    ///
    /// # Arguments
    ///
    /// * `authentication_flow` - The authentication flow to use (code, implicit, or hybrid).
    /// * `state_fn` - A function that returns an opaque value used by the client to maintain state
    ///   between the request and callback. The authorization server includes this value when
    ///   redirecting the user-agent back to the client.
    /// * `nonce_fn` - Similar to `state_fn`, but used to generate an opaque nonce to be used
    ///   when verifying the ID token returned by the OpenID Connect Provider.
    ///
    /// # Security Warning
    ///
    /// Callers should use a fresh, unpredictable `state` for each authorization request and verify
    /// that this value matches the `state` parameter passed by the authorization server to the
    /// redirect URI. Doing so mitigates
    /// [Cross-Site Request Forgery](https://tools.ietf.org/html/rfc6749#section-10.12)
    ///  attacks.
    ///
    /// Similarly, callers should use a fresh, unpredictable `nonce` to help protect against ID
    /// token reuse and forgery.
    pub fn authorize_url<NF, RS, SF>(
        &self,
        authentication_flow: AuthenticationFlow<RS>,
        state_fn: SF,
        nonce_fn: NF,
    ) -> AuthorizationRequest<'_, AD, P, RS>
    where
        NF: FnOnce() -> Nonce + 'static,
        RS: ResponseType,
        SF: FnOnce() -> CsrfToken + 'static,
    {
        let request = AuthorizationRequest {
            inner: self.oauth2_client.authorize_url(state_fn),
            acr_values: Vec::new(),
            authentication_flow,
            claims_locales: Vec::new(),
            display: None,
            id_token_hint: None,
            login_hint: None,
            max_age: None,
            nonce: nonce_fn(),
            prompts: Vec::new(),
            ui_locales: Vec::new(),
        };
        if self.use_openid_scope {
            request.add_scope(Scope::new(OPENID_SCOPE.to_string()))
        } else {
            request
        }
    }
}

/// Methods requiring a token endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        EndpointSet,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Request an access token using the
    /// [Client Credentials Flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4).
    ///
    /// Requires [`set_token_uri()`](Self::set_token_uri) to have been previously
    /// called to set the token endpoint.
    pub fn exchange_client_credentials(&self) -> ClientCredentialsTokenRequest<'_, TE, TR> {
        self.oauth2_client.exchange_client_credentials()
    }

    /// Exchange a code returned during the
    /// [Authorization Code Flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.1)
    /// for an access token.
    ///
    /// Acquires ownership of the `code` because authorization codes may only be used once to
    /// retrieve an access token from the authorization server.
    ///
    /// Requires [`set_token_uri()`](Self::set_token_uri) to have been previously
    /// called to set the token endpoint.
    pub fn exchange_code(&self, code: AuthorizationCode) -> CodeTokenRequest<'_, TE, TR> {
        self.oauth2_client.exchange_code(code)
    }

    /// Exchange an [RFC 8628](https://tools.ietf.org/html/rfc8628#section-3.2) Device Authorization
    /// Response returned by [`exchange_device_code()`](Self::exchange_device_code) for an access
    /// token.
    ///
    /// Requires [`set_token_uri()`](Self::set_token_uri) to have been previously
    /// called to set the token endpoint.
    pub fn exchange_device_access_token<'a, EF>(
        &'a self,
        auth_response: &'a DeviceAuthorizationResponse<EF>,
    ) -> DeviceAccessTokenRequest<'a, 'static, TR, EF>
    where
        EF: ExtraDeviceAuthorizationFields,
    {
        self.oauth2_client
            .exchange_device_access_token(auth_response)
    }

    /// Request an access token using the
    /// [Resource Owner Password Credentials Flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.3).
    ///
    /// Requires
    /// [`set_token_uri()`](Self::set_token_uri) to have
    /// been previously called to set the token endpoint.
    pub fn exchange_password<'a>(
        &'a self,
        username: &'a ResourceOwnerUsername,
        password: &'a ResourceOwnerPassword,
    ) -> PasswordTokenRequest<'a, TE, TR> {
        self.oauth2_client.exchange_password(username, password)
    }

    /// Exchange a refresh token for an access token.
    ///
    /// See <https://tools.ietf.org/html/rfc6749#section-6>.
    ///
    /// Requires
    /// [`set_token_uri()`](Self::set_token_uri) to have
    /// been previously called to set the token endpoint.
    pub fn exchange_refresh_token<'a>(
        &'a self,
        refresh_token: &'a RefreshToken,
    ) -> RefreshTokenRequest<'a, TE, TR> {
        self.oauth2_client.exchange_refresh_token(refresh_token)
    }

    /// Return the token endpoint.
    pub fn token_uri(&self) -> &TokenUrl {
        self.oauth2_client.token_uri()
    }
}

/// Methods with a possibly-set token endpoint after calling
/// [`from_provider_metadata()`](Self::from_provider_metadata).
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        EndpointMaybeSet,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Request an access token using the
    /// [Client Credentials Flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4).
    ///
    /// Requires [`from_provider_metadata()`](Self::from_provider_metadata) to have been previously
    /// called to construct the client.
    pub fn exchange_client_credentials(
        &self,
    ) -> Result<ClientCredentialsTokenRequest<'_, TE, TR>, ConfigurationError> {
        self.oauth2_client.exchange_client_credentials()
    }

    /// Exchange a code returned during the
    /// [Authorization Code Flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.1)
    /// for an access token.
    ///
    /// Acquires ownership of the `code` because authorization codes may only be used once to
    /// retrieve an access token from the authorization server.
    ///
    /// Requires [`from_provider_metadata()`](Self::from_provider_metadata) to have been previously
    /// called to construct the client.
    pub fn exchange_code(
        &self,
        code: AuthorizationCode,
    ) -> Result<CodeTokenRequest<'_, TE, TR>, ConfigurationError> {
        self.oauth2_client.exchange_code(code)
    }

    /// Exchange an [RFC 8628](https://tools.ietf.org/html/rfc8628#section-3.2) Device Authorization
    /// Response returned by [`exchange_device_code()`](Self::exchange_device_code) for an access
    /// token.
    ///
    /// Requires [`from_provider_metadata()`](Self::from_provider_metadata) to have been previously
    /// called to construct the client.
    pub fn exchange_device_access_token<'a, EF>(
        &'a self,
        auth_response: &'a DeviceAuthorizationResponse<EF>,
    ) -> Result<DeviceAccessTokenRequest<'a, 'static, TR, EF>, ConfigurationError>
    where
        EF: ExtraDeviceAuthorizationFields,
    {
        self.oauth2_client
            .exchange_device_access_token(auth_response)
    }

    /// Request an access token using the
    /// [Resource Owner Password Credentials Flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.3).
    ///
    /// Requires [`from_provider_metadata()`](Self::from_provider_metadata) to have been previously
    /// called to construct the client.
    pub fn exchange_password<'a>(
        &'a self,
        username: &'a ResourceOwnerUsername,
        password: &'a ResourceOwnerPassword,
    ) -> Result<PasswordTokenRequest<'a, TE, TR>, ConfigurationError> {
        self.oauth2_client.exchange_password(username, password)
    }

    /// Exchange a refresh token for an access token.
    ///
    /// See <https://tools.ietf.org/html/rfc6749#section-6>.
    ///
    /// Requires [`from_provider_metadata()`](Self::from_provider_metadata) to have been previously
    /// called to construct the client.
    pub fn exchange_refresh_token<'a>(
        &'a self,
        refresh_token: &'a RefreshToken,
    ) -> Result<RefreshTokenRequest<'a, TE, TR>, ConfigurationError> {
        self.oauth2_client.exchange_refresh_token(refresh_token)
    }

    /// Return the token endpoint.
    pub fn token_uri(&self) -> Option<&TokenUrl> {
        self.oauth2_client.token_uri()
    }
}

/// Methods requiring a device authorization endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        EndpointSet,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Begin the [RFC 8628](https://tools.ietf.org/html/rfc8628) Device Authorization Flow and
    /// retrieve a Device Authorization Response.
    ///
    /// Requires
    /// [`set_device_authorization_url()`](Self::set_device_authorization_url) to have
    /// been previously called to set the device authorization endpoint.
    ///
    /// See [`exchange_device_access_token()`](Self::exchange_device_access_token).
    pub fn exchange_device_code(&self) -> DeviceAuthorizationRequest<'_, TE> {
        let request = self.oauth2_client.exchange_device_code();
        if self.use_openid_scope {
            request.add_scope(Scope::new(OPENID_SCOPE.to_string()))
        } else {
            request
        }
    }

    /// Return the [RFC 8628](https://tools.ietf.org/html/rfc8628) device authorization endpoint
    /// used for the Device Authorization Flow.
    ///
    /// See [`exchange_device_code()`](Self::exchange_device_code).
    pub fn device_authorization_url(&self) -> &DeviceAuthorizationUrl {
        self.oauth2_client.device_authorization_url()
    }
}

/// Methods requiring an introspection endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        EndpointSet,
        HasRevocationUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Retrieve metadata for an access token using the
    /// [`RFC 7662`](https://tools.ietf.org/html/rfc7662) introspection endpoint.
    ///
    /// Requires [`set_introspection_url()`](Self::set_introspection_url) to have been previously
    /// called to set the introspection endpoint.
    pub fn introspect<'a>(&'a self, token: &'a AccessToken) -> IntrospectionRequest<'a, TE, TIR> {
        self.oauth2_client.introspect(token)
    }

    /// Return the [RFC 7662](https://tools.ietf.org/html/rfc7662) introspection endpoint.
    pub fn introspection_url(&self) -> &IntrospectionUrl {
        self.oauth2_client.introspection_url()
    }
}

/// Methods requiring a revocation endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasTokenUrl,
        HasUserInfoUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        EndpointSet,
        HasTokenUrl,
        HasUserInfoUrl,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasTokenUrl: EndpointState,
    HasUserInfoUrl: EndpointState,
{
    /// Revoke an access or refresh token using the [RFC 7009](https://tools.ietf.org/html/rfc7009)
    /// revocation endpoint.
    ///
    /// Requires [`set_revocation_url()`](Self::set_revocation_url) to have been previously
    /// called to set the revocation endpoint.
    pub fn revoke_token(
        &self,
        token: RT,
    ) -> Result<RevocationRequest<'_, RT, TRE>, ConfigurationError> {
        self.oauth2_client.revoke_token(token)
    }

    /// Return the [RFC 7009](https://tools.ietf.org/html/rfc7009) revocation endpoint.
    ///
    /// See [`revoke_token()`](Self::revoke_token()).
    pub fn revocation_url(&self) -> &RevocationUrl {
        self.oauth2_client.revocation_url()
    }
}

/// Methods requiring a user info endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        EndpointSet,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
{
    /// Request info about the user associated with the given access token.
    ///
    /// Requires [`set_user_info_url()`](Self::set_user_info_url) to have been previously
    /// called to set the user info endpoint.
    ///
    /// To help protect against token substitution attacks, this function optionally allows clients
    /// to provide the subject identifier whose user info they expect to receive. If provided and
    /// the subject returned by the OpenID Connect Provider does not match, the
    /// [`UserInfoRequest::request`] or [`UserInfoRequest::request_async`] functions will return
    /// [`UserInfoError::ClaimsVerification`](crate::UserInfoError::ClaimsVerification). If set to
    /// `None`, any subject is accepted.
    pub fn user_info(
        &self,
        access_token: AccessToken,
        expected_subject: Option<SubjectIdentifier>,
    ) -> UserInfoRequest<'_, JE, K> {
        self.user_info_impl(self.user_info_url(), access_token, expected_subject)
    }

    /// Return the user info endpoint.
    ///
    /// See ['user_info()'](Self::user_info).
    pub fn user_info_url(&self) -> &UserInfoUrl {
        // This is enforced statically via the HasUserInfo generic type.
        self.userinfo_endpoint
            .as_ref()
            .expect("should have user info endpoint")
    }
}

/// Methods with a possibly-set user info endpoint.
impl<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
    >
    Client<
        AC,
        AD,
        GC,
        JE,
        K,
        P,
        TE,
        TR,
        TIR,
        RT,
        TRE,
        HasAuthUrl,
        HasDeviceAuthUrl,
        HasIntrospectionUrl,
        HasRevocationUrl,
        HasTokenUrl,
        EndpointMaybeSet,
    >
where
    AC: AdditionalClaims,
    AD: AuthDisplay,
    GC: GenderClaim,
    JE: JweContentEncryptionAlgorithm<
        KeyType = <K::SigningAlgorithm as JwsSigningAlgorithm>::KeyType,
    >,
    K: JsonWebKey,
    P: AuthPrompt,
    TE: ErrorResponse + 'static,
    TR: TokenResponse<AC, GC, JE, K::SigningAlgorithm>,
    TIR: TokenIntrospectionResponse,
    RT: RevocableToken,
    TRE: ErrorResponse + 'static,
    HasAuthUrl: EndpointState,
    HasDeviceAuthUrl: EndpointState,
    HasIntrospectionUrl: EndpointState,
    HasRevocationUrl: EndpointState,
    HasTokenUrl: EndpointState,
{
    /// Request info about the user associated with the given access token.
    ///
    /// Requires [`from_provider_metadata()`](Self::from_provider_metadata) to have been previously
    /// called to construct the client.
    ///
    /// To help protect against token substitution attacks, this function optionally allows clients
    /// to provide the subject identifier whose user info they expect to receive. If provided and
    /// the subject returned by the OpenID Connect Provider does not match, the
    /// [`UserInfoRequest::request`] or [`UserInfoRequest::request_async`] functions will return
    /// [`UserInfoError::ClaimsVerification`](crate::UserInfoError::ClaimsVerification). If set to
    /// `None`, any subject is accepted.
    pub fn user_info(
        &self,
        access_token: AccessToken,
        expected_subject: Option<SubjectIdentifier>,
    ) -> Result<UserInfoRequest<'_, JE, K>, ConfigurationError> {
        Ok(self.user_info_impl(
            self.userinfo_endpoint
                .as_ref()
                .ok_or(ConfigurationError::MissingUrl("user info"))?,
            access_token,
            expected_subject,
        ))
    }

    /// Return the user info endpoint.
    ///
    /// See ['user_info()'](Self::user_info).
    pub fn user_info_url(&self) -> Option<&UserInfoUrl> {
        self.userinfo_endpoint.as_ref()
    }
}
