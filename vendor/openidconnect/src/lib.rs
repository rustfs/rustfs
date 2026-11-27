#![warn(missing_docs)]
#![allow(clippy::unreadable_literal, clippy::type_complexity)]
#![cfg_attr(test, allow(clippy::cognitive_complexity))]
//!
//! [OpenID Connect](https://openid.net/specs/openid-connect-core-1_0.html) library.
//!
//! This library provides extensible, strongly-typed interfaces for the OpenID Connect protocol.
//! For convenience, the [`core`] module provides type aliases for common usage that adheres to the
//! [OpenID Connect Core](https://openid.net/specs/openid-connect-core-1_0.html) spec. Users of
//! this crate may define their own extensions and custom type parameters in lieu of using the
//! [`core`] module.
//!
//! # Contents
//!  * [Importing `openidconnect`: selecting an HTTP client interface](#importing-openidconnect-selecting-an-http-client-interface)
//!  * [OpenID Connect Relying Party (Client) Interface](#openid-connect-relying-party-client-interface)
//!    * [Examples](#examples)
//!    * [Getting started: Authorization Code Grant w/ PKCE](#getting-started-authorization-code-grant-w-pkce)
//!  * [OpenID Connect Provider (Server) Interface](#openid-connect-provider-server-interface)
//!    * [OpenID Connect Discovery document](#openid-connect-discovery-document)
//!    * [OpenID Connect Discovery JSON Web Key Set](#openid-connect-discovery-json-web-key-set)
//!    * [OpenID Connect ID Token](#openid-connect-id-token)
//!  * [Asynchronous API](#asynchronous-api)
//!
//! # Importing `openidconnect`: selecting an HTTP client interface
//!
//!
//! This library offers a flexible HTTP client interface with two modes:
//!  * **Synchronous (blocking)**
//!
//!    NOTE: Be careful not to use a blocking HTTP client within `async` Rust code, which may panic
//!    or cause other issues. The
//!    [`tokio::task::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html)
//!    function may be useful in this situation.
//!  * **Asynchronous**
//!
//! ## Security Warning
//!
//! To prevent
//! [SSRF](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)
//! vulnerabilities, be sure to configure the HTTP client **not to follow redirects**. For example,
//! use [`redirect::Policy::none`](reqwest::redirect::Policy::none) when using
//! [`reqwest`], or [`redirects(0)`](ureq::AgentBuilder::redirects) when using [`ureq`].
//!
//! ## HTTP Clients
//!
//! For the HTTP client modes described above, the following HTTP client implementations can be
//! used:
//!  * **[`reqwest`]**
//!
//!    The `reqwest` HTTP client supports both the synchronous and asynchronous modes and is enabled
//!    by default.
//!
//!    Synchronous client: [`reqwest::blocking::Client`]
//!    (requires the `reqwest-blocking` feature flag)
//!
//!    Asynchronous client: [`reqwest::Client`] (requires either
//!    the `reqwest` or `reqwest-blocking` feature flags)
//!
//!  * **[`curl`]**
//!
//!    The `curl` HTTP client only supports the synchronous HTTP client mode and can be enabled in
//!    `Cargo.toml` via the `curl` feature flag.
//!
//!    Synchronous client: [`CurlHttpClient`]
//!
//!  * **[`ureq`]**
//!
//!    The `ureq` HTTP client is a simple HTTP client with minimal dependencies. It only supports
//!    the synchronous HTTP client mode and can be enabled in `Cargo.toml` via the `ureq` feature
//!    flag.
//!
//!    Synchronous client: [`ureq::Agent`]
//!
//!  * **Custom**
//!
//!    In addition to the clients above, users may define their own HTTP clients, which must accept
//!    an [`HttpRequest`] and return an [`HttpResponse`] or error. Users writing their own clients
//!    may wish to disable the default `reqwest` dependency by specifying
//!    `default-features = false` in `Cargo.toml` (replacing `...` with the desired version of this
//!    crate):
//!    ```toml
//!    openidconnect = { version = "...", default-features = false }
//!    ```
//!
//!    Synchronous HTTP clients should implement the [`SyncHttpClient`] trait, which is
//!    automatically implemented for any function/closure that implements:
//!    ```rust,ignore
//!    Fn(HttpRequest) -> Result<HttpResponse, E>
//!    where
//!      E: std::error::Error + 'static
//!    ```
//!
//!    Asynchronous HTTP clients should implement the [`AsyncHttpClient`] trait, which is
//!    automatically implemented for any function/closure that implements:
//!    ```rust,ignore
//!    Fn(HttpRequest) -> F
//!    where
//!      E: std::error::Error + 'static,
//!      F: Future<Output = Result<HttpResponse, E>>,
//!    ```
//!
//! # Comparing secrets securely
//!
//! OpenID Connect flows require comparing secrets received from providers. To do so securely
//! while avoiding [timing side-channels](https://en.wikipedia.org/wiki/Timing_attack), the
//! comparison must be done in constant time, either using a constant-time crate such as
//! [`constant_time_eq`](https://crates.io/crates/constant_time_eq) (which could break if a future
//! compiler version decides to be overly smart
//! about its optimizations), or by first computing a cryptographically-secure hash (e.g., SHA-256)
//! of both values and then comparing the hashes using `==`.
//!
//! The `timing-resistant-secret-traits` feature flag adds a safe (but comparatively expensive)
//! [`PartialEq`] implementation to the secret types. Timing side-channels are why [`PartialEq`] is
//! not auto-derived for this crate's secret types, and the lack of [`PartialEq`] is intended to
//! prompt users to think more carefully about these comparisons.
//!
//! # OpenID Connect Relying Party (Client) Interface
//!
//! The [`Client`] struct provides the OpenID Connect Relying Party interface. The most common
//! usage is provided by the [`core::CoreClient`] type alias.
//!
//! ## Examples
//!
//! * [Google](https://github.com/ramosbugs/openidconnect-rs/tree/main/examples/google.rs)
//!
//! ## Getting started: Authorization Code Grant w/ PKCE
//!
//! This is the most common OIDC/OAuth2 flow. PKCE is recommended whenever the client has no
//! client secret or has a client secret that cannot remain confidential (e.g., native, mobile, or
//! client-side web applications).
//!
//! ### Example
//!
//! ```rust,no_run
//! use anyhow::anyhow;
//! use openidconnect::{
//!     AccessTokenHash,
//!     AuthenticationFlow,
//!     AuthorizationCode,
//!     ClientId,
//!     ClientSecret,
//!     CsrfToken,
//!     IssuerUrl,
//!     Nonce,
//!     OAuth2TokenResponse,
//!     PkceCodeChallenge,
//!     RedirectUrl,
//!     Scope,
//!     TokenResponse,
//! };
//! use openidconnect::core::{
//!   CoreAuthenticationFlow,
//!   CoreClient,
//!   CoreProviderMetadata,
//!   CoreResponseType,
//!   CoreUserInfoClaims,
//! };
//! # #[cfg(feature = "reqwest-blocking")]
//! use openidconnect::reqwest;
//! use url::Url;
//!
//! # #[cfg(feature = "reqwest-blocking")]
//! # fn err_wrapper() -> Result<(), anyhow::Error> {
//! let http_client = reqwest::blocking::ClientBuilder::new()
//!     // Following redirects opens the client up to SSRF vulnerabilities.
//!     .redirect(reqwest::redirect::Policy::none())
//!     .build()
//!     .expect("Client should build");
//!
//! // Use OpenID Connect Discovery to fetch the provider metadata.
//! let provider_metadata = CoreProviderMetadata::discover(
//!     &IssuerUrl::new("https://accounts.example.com".to_string())?,
//!     &http_client,
//! )?;
//!
//! // Create an OpenID Connect client by specifying the client ID, client secret, authorization URL
//! // and token URL.
//! let client =
//!     CoreClient::from_provider_metadata(
//!         provider_metadata,
//!         ClientId::new("client_id".to_string()),
//!         Some(ClientSecret::new("client_secret".to_string())),
//!     )
//!     // Set the URL the user will be redirected to after the authorization process.
//!     .set_redirect_uri(RedirectUrl::new("http://redirect".to_string())?);
//!
//! // Generate a PKCE challenge.
//! let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
//!
//! // Generate the full authorization URL.
//! let (auth_url, csrf_token, nonce) = client
//!     .authorize_url(
//!         CoreAuthenticationFlow::AuthorizationCode,
//!         CsrfToken::new_random,
//!         Nonce::new_random,
//!     )
//!     // Set the desired scopes.
//!     .add_scope(Scope::new("read".to_string()))
//!     .add_scope(Scope::new("write".to_string()))
//!     // Set the PKCE code challenge.
//!     .set_pkce_challenge(pkce_challenge)
//!     .url();
//!
//! // This is the URL you should redirect the user to, in order to trigger the authorization
//! // process.
//! println!("Browse to: {}", auth_url);
//!
//! // Once the user has been redirected to the redirect URL, you'll have access to the
//! // authorization code. For security reasons, your code should verify that the `state`
//! // parameter returned by the server matches `csrf_state`.
//!
//! // Now you can exchange it for an access token and ID token.
//! let token_response =
//!     client
//!         .exchange_code(AuthorizationCode::new("some authorization code".to_string()))?
//!         // Set the PKCE code verifier.
//!         .set_pkce_verifier(pkce_verifier)
//!         .request(&http_client)?;
//!
//! // Extract the ID token claims after verifying its authenticity and nonce.
//! let id_token = token_response
//!   .id_token()
//!   .ok_or_else(|| anyhow!("Server did not return an ID token"))?;
//! let id_token_verifier = client.id_token_verifier();
//! let claims = id_token.claims(&id_token_verifier, &nonce)?;
//!
//! // Verify the access token hash to ensure that the access token hasn't been substituted for
//! // another user's.
//! if let Some(expected_access_token_hash) = claims.access_token_hash() {
//!     let actual_access_token_hash = AccessTokenHash::from_token(
//!         token_response.access_token(),
//!         id_token.signing_alg()?,
//!         id_token.signing_key(&id_token_verifier)?,
//!     )?;
//!     if actual_access_token_hash != *expected_access_token_hash {
//!         return Err(anyhow!("Invalid access token"));
//!     }
//! }
//!
//! // The authenticated user's identity is now available. See the IdTokenClaims struct for a
//! // complete listing of the available claims.
//! println!(
//!     "User {} with e-mail address {} has authenticated successfully",
//!     claims.subject().as_str(),
//!     claims.email().map(|email| email.as_str()).unwrap_or("<not provided>"),
//! );
//!
//! // If available, we can use the user info endpoint to request additional information.
//!
//! // The user_info request uses the AccessToken returned in the token response. To parse custom
//! // claims, use UserInfoClaims directly (with the desired type parameters) rather than using the
//! // CoreUserInfoClaims type alias.
//! let userinfo: CoreUserInfoClaims = client
//!     .user_info(token_response.access_token().to_owned(), None)?
//!     .request(&http_client)
//!     .map_err(|err| anyhow!("Failed requesting user info: {}", err))?;
//!
//! // See the OAuth2TokenResponse trait for a listing of other available fields such as
//! // access_token() and refresh_token().
//!
//! # Ok(())
//! # }
//! ```
//!
//! # OpenID Connect Provider (Server) Interface
//!
//! This library does not implement a complete OpenID Connect Provider, which requires
//! functionality such as credential and session management. However, it does provide
//! strongly-typed interfaces for parsing and building OpenID Connect protocol messages.
//!
//! ## OpenID Connect Discovery document
//!
//! The [`ProviderMetadata`] struct implements the
//! [OpenID Connect Discovery document](https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig).
//! This data structure should be serialized to JSON and served via the
//! `GET .well-known/openid-configuration` path relative to your provider's issuer URL.
//!
//! ### Example
//!
//! ```rust,no_run
//! use openidconnect::{
//!     AuthUrl,
//!     EmptyAdditionalProviderMetadata,
//!     IssuerUrl,
//!     JsonWebKeySetUrl,
//!     ResponseTypes,
//!     Scope,
//!     TokenUrl,
//!     UserInfoUrl,
//! };
//! use openidconnect::core::{
//!     CoreClaimName,
//!     CoreJwsSigningAlgorithm,
//!     CoreProviderMetadata,
//!     CoreResponseType,
//!     CoreSubjectIdentifierType
//! };
//! use url::Url;
//!
//! # fn err_wrapper() -> Result<String, anyhow::Error> {
//! let provider_metadata = CoreProviderMetadata::new(
//!     // Parameters required by the OpenID Connect Discovery spec.
//!     IssuerUrl::new("https://accounts.example.com".to_string())?,
//!     AuthUrl::new("https://accounts.example.com/authorize".to_string())?,
//!     // Use the JsonWebKeySet struct to serve the JWK Set at this URL.
//!     JsonWebKeySetUrl::new("https://accounts.example.com/jwk".to_string())?,
//!     // Supported response types (flows).
//!     vec![
//!         // Recommended: support the code flow.
//!         ResponseTypes::new(vec![CoreResponseType::Code]),
//!         // Optional: support the implicit flow.
//!         ResponseTypes::new(vec![CoreResponseType::Token, CoreResponseType::IdToken])
//!         // Other flows including hybrid flows may also be specified here.
//!     ],
//!     // For user privacy, the Pairwise subject identifier type is preferred. This prevents
//!     // distinct relying parties (clients) from knowing whether their users represent the same
//!     // real identities. This identifier type is only useful for relying parties that don't
//!     // receive the 'email', 'profile' or other personally-identifying scopes.
//!     // The Public subject identifier type is also supported.
//!     vec![CoreSubjectIdentifierType::Pairwise],
//!     // Support the RS256 signature algorithm.
//!     vec![CoreJwsSigningAlgorithm::RsaSsaPssSha256],
//!     // OpenID Connect Providers may supply custom metadata by providing a struct that
//!     // implements the AdditionalProviderMetadata trait. This requires manually using the
//!     // generic ProviderMetadata struct rather than the CoreProviderMetadata type alias,
//!     // however.
//!     EmptyAdditionalProviderMetadata {},
//! )
//! // Specify the token endpoint (required for the code flow).
//! .set_token_endpoint(Some(TokenUrl::new("https://accounts.example.com/token".to_string())?))
//! // Recommended: support the user info endpoint.
//! .set_userinfo_endpoint(
//!     Some(UserInfoUrl::new("https://accounts.example.com/userinfo".to_string())?)
//! )
//! // Recommended: specify the supported scopes.
//! .set_scopes_supported(Some(vec![
//!     Scope::new("openid".to_string()),
//!     Scope::new("email".to_string()),
//!     Scope::new("profile".to_string()),
//! ]))
//! // Recommended: specify the supported ID token claims.
//! .set_claims_supported(Some(vec![
//!     // Providers may also define an enum instead of using CoreClaimName.
//!     CoreClaimName::new("sub".to_string()),
//!     CoreClaimName::new("aud".to_string()),
//!     CoreClaimName::new("email".to_string()),
//!     CoreClaimName::new("email_verified".to_string()),
//!     CoreClaimName::new("exp".to_string()),
//!     CoreClaimName::new("iat".to_string()),
//!     CoreClaimName::new("iss".to_string()),
//!     CoreClaimName::new("name".to_string()),
//!     CoreClaimName::new("given_name".to_string()),
//!     CoreClaimName::new("family_name".to_string()),
//!     CoreClaimName::new("picture".to_string()),
//!     CoreClaimName::new("locale".to_string()),
//! ]));
//!
//! serde_json::to_string(&provider_metadata).map_err(From::from)
//! # }
//! ```
//!
//! ## OpenID Connect Discovery JSON Web Key Set
//!
//! The JSON Web Key Set (JWKS) provides the public keys that relying parties (clients) use to
//! verify the authenticity of ID tokens returned by this OpenID Connect Provider. The
//! [`JsonWebKeySet`] data structure should be serialized as JSON and served at the URL specified
//! in the `jwks_uri` field of the [`ProviderMetadata`] returned in the OpenID Connect Discovery
//! document.
//!
//! ### Example
//!
//! ```rust,no_run
//! use openidconnect::{JsonWebKeyId, PrivateSigningKey};
//! use openidconnect::core::{CoreJsonWebKey, CoreJsonWebKeySet, CoreRsaPrivateSigningKey};
//!
//! # fn err_wrapper() -> Result<String, anyhow::Error> {
//! # let rsa_pem = "";
//! let jwks = CoreJsonWebKeySet::new(
//!     vec![
//!         // RSA keys may also be constructed directly using CoreJsonWebKey::new_rsa(). Providers
//!         // aiming to support other key types may provide their own implementation of the
//!         // JsonWebKey trait or submit a PR to add the desired support to this crate.
//!         CoreRsaPrivateSigningKey::from_pem(
//!             &rsa_pem,
//!             Some(JsonWebKeyId::new("key1".to_string()))
//!         )
//!         .expect("Invalid RSA private key")
//!         .as_verification_key()
//!     ]
//! );
//!
//! serde_json::to_string(&jwks).map_err(From::from)
//! # }
//! ```
//!
//! ## OpenID Connect ID Token
//!
//! The [`IdToken::new`] method is used for signing ID token claims, which can then be returned
//! from the token endpoint as part of the [`StandardTokenResponse`] struct
//! (or [`core::CoreTokenResponse`] type alias). The ID token can also be serialized to a string
//! using the `IdToken::to_string` method and returned directly from the authorization endpoint
//! when the implicit flow or certain hybrid flows are used. Note that in these flows, ID tokens
//! must only be returned in the URL fragment, and never as a query parameter.
//!
//! The ID token contains a combination of the
//! [OpenID Connect Standard Claims](https://openid.net/specs/openid-connect-core-1_0.html#StandardClaims)
//! (see [`StandardClaims`]) and claims specific to the
//! [OpenID Connect ID Token](https://openid.net/specs/openid-connect-core-1_0.html#IDToken)
//! (see [`IdTokenClaims`]).
//!
//! ### Example
//!
//! ```rust,no_run
//! use chrono::{Duration, Utc};
//! use openidconnect::{
//!     AccessToken,
//!     Audience,
//!     EmptyAdditionalClaims,
//!     EmptyExtraTokenFields,
//!     EndUserEmail,
//!     IssuerUrl,
//!     JsonWebKeyId,
//!     StandardClaims,
//!     SubjectIdentifier,
//! };
//! use openidconnect::core::{
//!     CoreIdToken,
//!     CoreIdTokenClaims,
//!     CoreIdTokenFields,
//!     CoreJwsSigningAlgorithm,
//!     CoreRsaPrivateSigningKey,
//!     CoreTokenResponse,
//!     CoreTokenType,
//! };
//!
//! # fn err_wrapper() -> Result<CoreTokenResponse, anyhow::Error> {
//! # let rsa_pem = "";
//! # let access_token = AccessToken::new("".to_string());
//! let id_token = CoreIdToken::new(
//!     CoreIdTokenClaims::new(
//!         // Specify the issuer URL for the OpenID Connect Provider.
//!         IssuerUrl::new("https://accounts.example.com".to_string())?,
//!         // The audience is usually a single entry with the client ID of the client for whom
//!         // the ID token is intended. This is a required claim.
//!         vec![Audience::new("client-id-123".to_string())],
//!         // The ID token expiration is usually much shorter than that of the access or refresh
//!         // tokens issued to clients.
//!         Utc::now() + Duration::seconds(300),
//!         // The issue time is usually the current time.
//!         Utc::now(),
//!         // Set the standard claims defined by the OpenID Connect Core spec.
//!         StandardClaims::new(
//!             // Stable subject identifiers are recommended in place of e-mail addresses or other
//!             // potentially unstable identifiers. This is the only required claim.
//!             SubjectIdentifier::new("5f83e0ca-2b8e-4e8c-ba0a-f80fe9bc3632".to_string())
//!         )
//!         // Optional: specify the user's e-mail address. This should only be provided if the
//!         // client has been granted the 'profile' or 'email' scopes.
//!         .set_email(Some(EndUserEmail::new("bob@example.com".to_string())))
//!         // Optional: specify whether the provider has verified the user's e-mail address.
//!         .set_email_verified(Some(true)),
//!         // OpenID Connect Providers may supply custom claims by providing a struct that
//!         // implements the AdditionalClaims trait. This requires manually using the
//!         // generic IdTokenClaims struct rather than the CoreIdTokenClaims type alias,
//!         // however.
//!         EmptyAdditionalClaims {},
//!     ),
//!     // The private key used for signing the ID token. For confidential clients (those able
//!     // to maintain a client secret), a CoreHmacKey can also be used, in conjunction
//!     // with one of the CoreJwsSigningAlgorithm::HmacSha* signing algorithms. When using an
//!     // HMAC-based signing algorithm, the UTF-8 representation of the client secret should
//!     // be used as the HMAC key.
//!     &CoreRsaPrivateSigningKey::from_pem(
//!             &rsa_pem,
//!             Some(JsonWebKeyId::new("key1".to_string()))
//!         )
//!         .expect("Invalid RSA private key"),
//!     // Uses the RS256 signature algorithm. This crate supports any RS*, PS*, or HS*
//!     // signature algorithm.
//!     CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
//!     // When returning the ID token alongside an access token (e.g., in the Authorization Code
//!     // flow), it is recommended to pass the access token here to set the `at_hash` claim
//!     // automatically.
//!     Some(&access_token),
//!     // When returning the ID token alongside an authorization code (e.g., in the implicit
//!     // flow), it is recommended to pass the authorization code here to set the `c_hash` claim
//!     // automatically.
//!     None,
//! )?;
//!
//! Ok(CoreTokenResponse::new(
//!     AccessToken::new("some_secret".to_string()),
//!     CoreTokenType::Bearer,
//!     CoreIdTokenFields::new(Some(id_token), EmptyExtraTokenFields {}),
//! ))
//! # }
//! ```
//!
//! # Asynchronous API
//!
//! An asynchronous API for async/await is also provided.
//!
//! ## Example
//!
//! ```rust,no_run
//! use anyhow::anyhow;
//! use openidconnect::{
//!     AccessTokenHash,
//!     AuthenticationFlow,
//!     AuthorizationCode,
//!     ClientId,
//!     ClientSecret,
//!     CsrfToken,
//!     IssuerUrl,
//!     Nonce,
//!     OAuth2TokenResponse,
//!     PkceCodeChallenge,
//!     RedirectUrl,
//!     Scope,
//!     TokenResponse,
//! };
//! use openidconnect::core::{
//!   CoreAuthenticationFlow,
//!   CoreClient,
//!   CoreProviderMetadata,
//!   CoreResponseType,
//! };
//! # #[cfg(feature = "reqwest")]
//! use openidconnect::reqwest;
//! use url::Url;
//!
//!
//! # #[cfg(feature = "reqwest")]
//! # async fn err_wrapper() -> Result<(), anyhow::Error> {
//! let http_client = reqwest::ClientBuilder::new()
//!     // Following redirects opens the client up to SSRF vulnerabilities.
//!     .redirect(reqwest::redirect::Policy::none())
//!     .build()
//!     .expect("Client should build");
//!
//! // Use OpenID Connect Discovery to fetch the provider metadata.
//! let provider_metadata = CoreProviderMetadata::discover_async(
//!     IssuerUrl::new("https://accounts.example.com".to_string())?,
//!     &http_client,
//! )
//! .await?;
//!
//! // Create an OpenID Connect client by specifying the client ID, client secret, authorization URL
//! // and token URL.
//! let client =
//!     CoreClient::from_provider_metadata(
//!         provider_metadata,
//!         ClientId::new("client_id".to_string()),
//!         Some(ClientSecret::new("client_secret".to_string())),
//!     )
//!     // Set the URL the user will be redirected to after the authorization process.
//!     .set_redirect_uri(RedirectUrl::new("http://redirect".to_string())?);
//!
//! // Generate a PKCE challenge.
//! let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
//!
//! // Generate the full authorization URL.
//! let (auth_url, csrf_token, nonce) = client
//!     .authorize_url(
//!         CoreAuthenticationFlow::AuthorizationCode,
//!         CsrfToken::new_random,
//!         Nonce::new_random,
//!     )
//!     // Set the desired scopes.
//!     .add_scope(Scope::new("read".to_string()))
//!     .add_scope(Scope::new("write".to_string()))
//!     // Set the PKCE code challenge.
//!     .set_pkce_challenge(pkce_challenge)
//!     .url();
//!
//! // This is the URL you should redirect the user to, in order to trigger the authorization
//! // process.
//! println!("Browse to: {}", auth_url);
//!
//! // Once the user has been redirected to the redirect URL, you'll have access to the
//! // authorization code. For security reasons, your code should verify that the `state`
//! // parameter returned by the server matches `csrf_state`.
//!
//! // Now you can exchange it for an access token and ID token.
//! let token_response =
//!     client
//!         .exchange_code(AuthorizationCode::new("some authorization code".to_string()))?
//!         // Set the PKCE code verifier.
//!         .set_pkce_verifier(pkce_verifier)
//!         .request_async(&http_client)
//!         .await?;
//!
//! // Extract the ID token claims after verifying its authenticity and nonce.
//! let id_token = token_response
//!   .id_token()
//!   .ok_or_else(|| anyhow!("Server did not return an ID token"))?;
//! let id_token_verifier = client.id_token_verifier();
//! let claims = id_token.claims(&id_token_verifier, &nonce)?;
//!
//! // Verify the access token hash to ensure that the access token hasn't been substituted for
//! // another user's.
//! if let Some(expected_access_token_hash) = claims.access_token_hash() {
//!     let actual_access_token_hash = AccessTokenHash::from_token(
//!         token_response.access_token(),
//!         id_token.signing_alg()?,
//!         id_token.signing_key(&id_token_verifier)?,
//!     )?;
//!     if actual_access_token_hash != *expected_access_token_hash {
//!         return Err(anyhow!("Invalid access token"));
//!     }
//! }
//!
//! // The authenticated user's identity is now available. See the IdTokenClaims struct for a
//! // complete listing of the available claims.
//! println!(
//!     "User {} with e-mail address {} has authenticated successfully",
//!     claims.subject().as_str(),
//!     claims.email().map(|email| email.as_str()).unwrap_or("<not provided>"),
//! );
//!
//! // See the OAuth2TokenResponse trait for a listing of other available fields such as
//! // access_token() and refresh_token().
//!
//! # Ok(())
//! # }
//! ```

use crate::jwt::{JsonWebToken, JsonWebTokenAccess, JsonWebTokenAlgorithm, JsonWebTokenHeader};
use crate::verification::{AudiencesClaim, IssuerClaim};

// Defined first since other modules need the macros, and definition order is significant for
// macros. This module is private.
#[macro_use]
mod macros;

/// Baseline OpenID Connect implementation and types.
pub mod core;

/// OpenID Connect Dynamic Client Registration.
pub mod registration;

// Private modules since we may move types between different modules; these are exported publicly
// via the pub use above.
mod authorization;
mod claims;
mod client;
mod discovery;
mod helpers;
mod id_token;
mod logout;
mod token;
mod types;
mod user_info;
mod verification;

// Private module for HTTP(S) utilities.
mod http_utils;

// Private module for JWT utilities.
mod jwt;

pub use oauth2::{
    AccessToken, AsyncHttpClient, AuthType, AuthUrl, AuthorizationCode,
    ClientCredentialsTokenRequest, ClientId, ClientSecret, CodeTokenRequest, ConfigurationError,
    CsrfToken, DeviceAccessTokenRequest, DeviceAuthorizationRequest, DeviceAuthorizationResponse,
    DeviceAuthorizationUrl, DeviceCode, DeviceCodeErrorResponse, DeviceCodeErrorResponseType,
    EmptyExtraDeviceAuthorizationFields, EmptyExtraTokenFields, EndUserVerificationUrl,
    EndpointMaybeSet, EndpointNotSet, EndpointSet, EndpointState, ErrorResponse, ErrorResponseType,
    ExtraDeviceAuthorizationFields, ExtraTokenFields, HttpClientError, HttpRequest, HttpResponse,
    IntrospectionRequest, IntrospectionUrl, PasswordTokenRequest, PkceCodeChallenge,
    PkceCodeChallengeMethod, PkceCodeVerifier, RedirectUrl, RefreshToken, RefreshTokenRequest,
    RequestTokenError, ResourceOwnerPassword, ResourceOwnerUsername, RevocableToken,
    RevocationErrorResponseType, RevocationRequest, RevocationUrl, Scope, StandardErrorResponse,
    StandardTokenIntrospectionResponse, StandardTokenResponse, SyncHttpClient,
    TokenIntrospectionResponse, TokenResponse as OAuth2TokenResponse, TokenType, TokenUrl,
    UserCode, VerificationUriComplete,
};

/// Public re-exports of types used for HTTP client interfaces.
pub use oauth2::http;
pub use oauth2::url;

#[cfg(all(feature = "curl", not(target_arch = "wasm32")))]
pub use oauth2::curl;

#[cfg(all(feature = "curl", not(target_arch = "wasm32")))]
pub use oauth2::CurlHttpClient;

#[cfg(all(feature = "curl", target_arch = "wasm32"))]
compile_error!("wasm32 is not supported with the `curl` feature. Use the `reqwest` backend or a custom backend for wasm32 support");

#[cfg(any(feature = "reqwest", feature = "reqwest-blocking"))]
pub use oauth2::reqwest;

#[cfg(feature = "ureq")]
pub use oauth2::ureq;

pub use crate::authorization::{AuthenticationFlow, AuthorizationRequest};
pub use crate::claims::{
    AdditionalClaims, AddressClaim, EmptyAdditionalClaims, GenderClaim, StandardClaims,
};
pub use crate::client::Client;
pub use crate::discovery::{
    AdditionalProviderMetadata, DiscoveryError, EmptyAdditionalProviderMetadata, ProviderMetadata,
};
pub use crate::id_token::IdTokenFields;
pub use crate::id_token::{IdToken, IdTokenClaims};
pub use crate::jwt::{JsonWebTokenError, JsonWebTokenType, NormalizedJsonWebTokenType};
pub use crate::logout::{LogoutProviderMetadata, LogoutRequest, ProviderMetadataWithLogout};
pub use crate::token::TokenResponse;
// Flatten the module hierarchy involving types. They're only separated to improve code
// organization.
pub use crate::types::jwk::{
    JsonWebKey, JsonWebKeyAlgorithm, JsonWebKeyId, JsonWebKeyType, JsonWebKeyUse,
    JweContentEncryptionAlgorithm, JweKeyManagementAlgorithm, JwsSigningAlgorithm,
    PrivateSigningKey,
};
pub use crate::types::jwks::{JsonWebKeySet, JsonWebKeySetUrl};
pub use crate::types::localized::{LanguageTag, LocalizedClaim};
pub use crate::types::{
    AccessTokenHash, AddressCountry, AddressLocality, AddressPostalCode, AddressRegion,
    ApplicationType, Audience, AuthDisplay, AuthPrompt, AuthenticationContextClass,
    AuthenticationMethodReference, AuthorizationCodeHash, ClaimName, ClaimType, ClientAuthMethod,
    ClientConfigUrl, ClientContactEmail, ClientName, ClientUrl, EndSessionUrl, EndUserBirthday,
    EndUserEmail, EndUserFamilyName, EndUserGivenName, EndUserMiddleName, EndUserName,
    EndUserNickname, EndUserPhoneNumber, EndUserPictureUrl, EndUserProfileUrl, EndUserTimezone,
    EndUserUsername, EndUserWebsiteUrl, FormattedAddress, GrantType, InitiateLoginUrl, IssuerUrl,
    LoginHint, LogoUrl, LogoutHint, Nonce, OpPolicyUrl, OpTosUrl, PolicyUrl, PostLogoutRedirectUrl,
    RegistrationAccessToken, RegistrationUrl, RequestUrl, ResponseMode, ResponseType,
    ResponseTypes, SectorIdentifierUrl, ServiceDocUrl, SigningError, StreetAddress,
    SubjectIdentifier, SubjectIdentifierType, ToSUrl,
};
pub use crate::user_info::{
    UserInfoClaims, UserInfoError, UserInfoJsonWebToken, UserInfoRequest, UserInfoResponseType,
    UserInfoUrl,
};
pub use crate::verification::{
    ClaimsVerificationError, IdTokenVerifier, NonceVerifier, SignatureVerificationError,
    UserInfoVerifier,
};
