use crate::http_utils::{check_content_type, MIME_TYPE_JSON, MIME_TYPE_JWKS};
use crate::types::jwk::{JsonWebKey, JsonWebKeyId, JwsSigningAlgorithm};
use crate::{
    AsyncHttpClient, DiscoveryError, HttpRequest, HttpResponse, JsonWebKeyUse, SyncHttpClient,
};

use http::header::ACCEPT;
use http::{HeaderValue, Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, VecSkipError};

use std::future::Future;

new_url_type![
    /// JSON Web Key Set URL.
    JsonWebKeySetUrl
];

/// JSON Web Key Set.
#[serde_as]
#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct JsonWebKeySet<K>
where
    K: JsonWebKey,
{
    // FIXME: write a test that ensures duplicate object member names cause an error
    // (see https://tools.ietf.org/html/rfc7517#section-5)
    #[serde(bound = "K: JsonWebKey")]
    // Ignores invalid keys rather than failing. That way, clients can function using the keys that
    // they do understand, which is fine if they only ever get JWTs signed with those keys.
    #[serde_as(as = "VecSkipError<_>")]
    keys: Vec<K>,
}

/// Checks whether a JWK key can be used with a given signing algorithm.
pub(crate) fn check_key_compatibility<K>(
    key: &K,
    signing_algorithm: &K::SigningAlgorithm,
) -> Result<(), &'static str>
where
    K: JsonWebKey,
{
    // if this key isn't suitable for signing
    if let Some(use_) = key.key_use() {
        if !use_.allows_signature() {
            return Err("key usage not permitted for digital signatures");
        }
    }

    // if this key doesn't have the right key type
    if signing_algorithm.key_type().as_ref() != Some(key.key_type()) {
        return Err("key type does not match signature algorithm");
    }

    match key.signing_alg() {
        // if no specific algorithm is mandated, any will do
        crate::JsonWebKeyAlgorithm::Unspecified => Ok(()),
        crate::JsonWebKeyAlgorithm::Unsupported => Err("key algorithm is not a signing algorithm"),
        crate::JsonWebKeyAlgorithm::Algorithm(key_alg) if key_alg == signing_algorithm => Ok(()),
        crate::JsonWebKeyAlgorithm::Algorithm(_) => Err("incompatible key algorithm"),
    }
}

impl<K> JsonWebKeySet<K>
where
    K: JsonWebKey,
{
    /// Create a new JSON Web Key Set.
    pub fn new(keys: Vec<K>) -> Self {
        Self { keys }
    }

    /// Return a list of suitable keys, given a key ID and signature algorithm
    pub(crate) fn filter_keys(
        &self,
        key_id: Option<&JsonWebKeyId>,
        signature_alg: &K::SigningAlgorithm,
    ) -> Vec<&K> {
        self.keys()
        .iter()
        .filter(|key|
            // Either the JWT doesn't include a 'kid' (in which case any 'kid'
            // is acceptable), or the 'kid' matches the key's ID.
            if key_id.is_some() && key_id != key.key_id() {
                false
            } else {
                check_key_compatibility(*key, signature_alg).is_ok()
            }
        )
        .collect()
    }

    /// Fetch a remote JSON Web Key Set from the specified `url` using the given `http_client`
    /// (e.g., [`reqwest::blocking::Client`](crate::reqwest::blocking::Client) or
    /// [`CurlHttpClient`](crate::CurlHttpClient)).
    pub fn fetch<C>(
        url: &JsonWebKeySetUrl,
        http_client: &C,
    ) -> Result<Self, DiscoveryError<<C as SyncHttpClient>::Error>>
    where
        C: SyncHttpClient,
    {
        http_client
            .call(Self::fetch_request(url).map_err(|err| {
                DiscoveryError::Other(format!("failed to prepare request: {err}"))
            })?)
            .map_err(DiscoveryError::Request)
            .and_then(Self::fetch_response)
    }

    /// Fetch a remote JSON Web Key Set from the specified `url` using the given async `http_client`
    /// (e.g., [`reqwest::Client`](crate::reqwest::Client)).
    pub fn fetch_async<'c, C>(
        url: &JsonWebKeySetUrl,
        http_client: &'c C,
    ) -> impl Future<Output = Result<Self, DiscoveryError<<C as AsyncHttpClient<'c>>::Error>>> + 'c
    where
        Self: 'c,
        C: AsyncHttpClient<'c>,
    {
        let fetch_request = Self::fetch_request(url)
            .map_err(|err| DiscoveryError::Other(format!("failed to prepare request: {err}")));
        Box::pin(async move {
            http_client
                .call(fetch_request?)
                .await
                .map_err(DiscoveryError::Request)
                .and_then(Self::fetch_response)
        })
    }

    fn fetch_request(url: &JsonWebKeySetUrl) -> Result<HttpRequest, http::Error> {
        http::Request::builder()
            .uri(url.to_string())
            .method(Method::GET)
            .header(ACCEPT, HeaderValue::from_static(MIME_TYPE_JSON))
            .body(Vec::new())
    }

    fn fetch_response<RE>(http_response: HttpResponse) -> Result<Self, DiscoveryError<RE>>
    where
        RE: std::error::Error + 'static,
    {
        if http_response.status() != StatusCode::OK {
            return Err(DiscoveryError::Response(
                http_response.status(),
                http_response.body().to_owned(),
                format!("HTTP status code {}", http_response.status()),
            ));
        }

        check_content_type(http_response.headers(), MIME_TYPE_JSON)
            .or_else(|err| {
                check_content_type(http_response.headers(), MIME_TYPE_JWKS).map_err(|_| err)
            })
            .map_err(|err_msg| {
                DiscoveryError::Response(
                    http_response.status(),
                    http_response.body().to_owned(),
                    err_msg,
                )
            })?;

        serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_slice(
            http_response.body(),
        ))
        .map_err(DiscoveryError::Parse)
    }

    /// Return the keys in this JSON Web Key Set.
    pub fn keys(&self) -> &Vec<K> {
        &self.keys
    }
}
impl<K> Clone for JsonWebKeySet<K>
where
    K: JsonWebKey,
{
    fn clone(&self) -> Self {
        Self::new(self.keys.clone())
    }
}
impl<K> Default for JsonWebKeySet<K>
where
    K: JsonWebKey,
{
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
