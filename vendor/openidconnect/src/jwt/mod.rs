use crate::{
    JsonWebKey, JsonWebKeyId, JweContentEncryptionAlgorithm, JwsSigningAlgorithm,
    PrivateSigningKey, SignatureVerificationError, SigningError,
};

use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use serde::de::{DeserializeOwned, Error as _, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::skip_serializing_none;
use thiserror::Error;

use std::fmt::Debug;
use std::marker::PhantomData;

#[cfg(test)]
pub(crate) mod tests;

new_type![
    #[derive(Deserialize, Hash, Ord, PartialOrd, Serialize)]
    JsonWebTokenContentType(String)
];

/// Error type used when normalizing [`JsonWebTokenType`] objects
#[derive(Error, Debug)]
#[error("Invalid JWT type: {typ}")]
pub struct InvalidJsonWebTokenTypeError {
    typ: String,
}

new_type![
    /// JSON Web Token type field (typ)
    ///
    /// This type stores the raw (deserialized) value.
    ///
    /// To compare two different JSON Web Token types, please use the normalized version via [`JsonWebTokenType::normalize`].
    #[derive(Deserialize, Hash, Serialize)]
    JsonWebTokenType(String)

    impl {
        /// Expands a [`JsonWebTokenType`] and produces a [`NormalizedJsonWebTokenType`] according to RFC2045 and RFC7515.
        ///
        /// See [RFC 2045 section 5.1](https://tools.ietf.org/html/rfc2045#section-5.1) for the full Content-Type Header Field spec.
        /// See [RFC 7515 section 4.19](https://tools.ietf.org/html/rfc7515#section-4.1.9) for specific requirements of JSON Web Token Types.
        pub fn normalize(&self) -> Result<NormalizedJsonWebTokenType, InvalidJsonWebTokenTypeError> {
            self.try_into()
        }
    }
];

/// Normalized JSON Web Token type field (typ)
///
/// This type stores the normalized value of a [`JsonWebTokenType`].
/// To retrieve a normalized value according to RFC2045 and RFC7515 see [`JsonWebTokenType::normalize`]
///
/// See [RFC 2045 section 5.1](https://tools.ietf.org/html/rfc2045#section-5.1) for the full Content-Type Header Field spec.
/// See [RFC 7515 section 4.1.9](https://tools.ietf.org/html/rfc7515#section-4.1.9) for specific requirements of JSON Web Token Types.
///
/// It is recommended to instantiate `NormalizedJsonWebTokenType` objects via [`JsonWebTokenType`] and then call [`JsonWebTokenType::normalize`].
///
/// ```rust
/// # use openidconnect::{NormalizedJsonWebTokenType, JsonWebTokenType};
/// let token_type = JsonWebTokenType::new("jwt+at".to_string()).normalize();
/// // normalized value looks like "application/jwt+at"
/// # assert_eq!(*token_type.unwrap(), "application/jwt+at")
/// ```
#[derive(Clone, Debug, Deserialize, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct NormalizedJsonWebTokenType(String);

impl std::ops::Deref for NormalizedJsonWebTokenType {
    type Target = String;
    fn deref(&self) -> &String {
        &self.0
    }
}
impl From<NormalizedJsonWebTokenType> for String {
    fn from(t: NormalizedJsonWebTokenType) -> String {
        t.0
    }
}

impl From<NormalizedJsonWebTokenType> for JsonWebTokenType {
    fn from(t: NormalizedJsonWebTokenType) -> JsonWebTokenType {
        JsonWebTokenType::new(t.0)
    }
}

impl TryFrom<&JsonWebTokenType> for NormalizedJsonWebTokenType {
    type Error = InvalidJsonWebTokenTypeError;

    /// Normalizes a [`JsonWebTokenType`] and produces a [`NormalizedJsonWebTokenType`] according to RFC2045.
    ///
    /// See [RFC 2045 section 5.1](https://tools.ietf.org/html/rfc2045#section-5.1) for the full Content-Type Header Field spec.
    /// See [RFC 7515 section 4.19](https://tools.ietf.org/html/rfc7515#section-4.1.9) for specific requirements of JSON Web Token Types.
    fn try_from(t: &JsonWebTokenType) -> Result<NormalizedJsonWebTokenType, Self::Error> {
        let lowercase_jwt_type = t.0.to_lowercase();
        if let Some(slash_location) = lowercase_jwt_type.find('/') {
            if let Some(semicolon_location) = lowercase_jwt_type.find(';') {
                // If '/' is not before ';' as then the MIME type is invalid
                // e.g. some;arg="1/2" is invalid, but application/some;arg=1 is valid
                // OR
                // If MIME type has not at least one character
                // OR
                // If MIME subtype has not at least one character
                if slash_location > semicolon_location
                    || slash_location == 0
                    || slash_location.saturating_add(1) >= semicolon_location
                {
                    Err(InvalidJsonWebTokenTypeError {
                        typ: lowercase_jwt_type,
                    })
                } else {
                    Ok(NormalizedJsonWebTokenType(lowercase_jwt_type))
                }
            } else {
                Ok(NormalizedJsonWebTokenType(lowercase_jwt_type))
            }
        } else {
            Ok(NormalizedJsonWebTokenType(format!(
                "application/{lowercase_jwt_type}"
            )))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum JsonWebTokenAlgorithm<JE, JS>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
{
    Encryption(JE),
    Signature(JS),
    /// No digital signature or MAC performed.
    ///
    /// # Security Warning
    ///
    /// This algorithm provides no security over the integrity of the JSON Web Token. Clients
    /// should be careful not to rely on unsigned JWT's for security purposes. See
    /// [Critical vulnerabilities in JSON Web Token libraries](
    ///     https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/) for
    /// further discussion.
    None,
}
impl<'de, JE, JS> Deserialize<'de> for JsonWebTokenAlgorithm<JE, JS>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
        // TODO: get rid of this clone() (see below)
        let s: String = serde_json::from_value(value.clone()).map_err(D::Error::custom)?;

        // NB: These comparisons are case-sensitive. Section 4.1.1 of RFC 7515 states: "The "alg"
        // value is a case-sensitive ASCII string containing a StringOrURI value."
        if s == "none" {
            Ok(JsonWebTokenAlgorithm::None)
        // TODO: Figure out a way to deserialize the enums without giving up ownership
        } else if let Ok(val) = serde_json::from_value::<JE>(value.clone()) {
            Ok(JsonWebTokenAlgorithm::Encryption(val))
        } else if let Ok(val) = serde_json::from_value::<JS>(value) {
            Ok(JsonWebTokenAlgorithm::Signature(val))
        } else {
            Err(D::Error::custom(format!(
                "unrecognized JSON Web Algorithm `{s}`"
            )))
        }
    }
}
impl<JE, JS> Serialize for JsonWebTokenAlgorithm<JE, JS>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
{
    fn serialize<SE>(&self, serializer: SE) -> Result<SE::Ok, SE::Error>
    where
        SE: Serializer,
    {
        match self {
            JsonWebTokenAlgorithm::Encryption(ref enc) => enc.serialize(serializer),
            JsonWebTokenAlgorithm::Signature(ref sig) => sig.serialize(serializer),
            JsonWebTokenAlgorithm::None => serializer.serialize_str("none"),
        }
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct JsonWebTokenHeader<JE, JS>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
{
    #[serde(
        bound = "JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>, JS: JwsSigningAlgorithm"
    )]
    pub alg: JsonWebTokenAlgorithm<JE, JS>,
    // Additional critical header parameters that must be understood by this implementation. Since
    // we don't understand any such extensions, we reject any JWT with this value present (the
    // spec specifically prohibits including public (standard) headers in this field).
    // See https://tools.ietf.org/html/rfc7515#section-4.1.11.
    pub crit: Option<Vec<String>>,
    pub cty: Option<JsonWebTokenContentType>,
    pub kid: Option<JsonWebKeyId>,
    pub typ: Option<JsonWebTokenType>,
    // Other JOSE header fields are omitted since the OpenID Connect spec specifically says that
    // the "x5u", "x5c", "jku", "jwk" header parameter fields SHOULD NOT be used.
    // See http://openid.net/specs/openid-connect-core-1_0-final.html#IDToken.
}

pub trait JsonWebTokenPayloadSerde<P>: Debug
where
    P: Debug + DeserializeOwned + Serialize,
{
    fn deserialize<DE: serde::de::Error>(payload: &[u8]) -> Result<P, DE>;
    fn serialize(payload: &P) -> Result<String, serde_json::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonWebTokenJsonPayloadSerde;
impl<P> JsonWebTokenPayloadSerde<P> for JsonWebTokenJsonPayloadSerde
where
    P: Debug + DeserializeOwned + Serialize,
{
    fn deserialize<DE: serde::de::Error>(payload: &[u8]) -> Result<P, DE> {
        serde_json::from_slice(payload)
            .map_err(|err| DE::custom(format!("Failed to parse payload JSON: {err:?}")))
    }

    fn serialize(payload: &P) -> Result<String, serde_json::Error> {
        serde_json::to_string(payload)
    }
}

// Helper trait so that we can get borrowed payload when we have a reference to the JWT and owned
// payload when we own the JWT.
pub trait JsonWebTokenAccess<JE, JS, P>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
{
    type ReturnType;

    fn unverified_header(&self) -> &JsonWebTokenHeader<JE, JS>;
    fn unverified_payload(self) -> Self::ReturnType;
    fn unverified_payload_ref(&self) -> &P;

    fn payload<K>(
        self,
        signature_alg: &JS,
        key: &K,
    ) -> Result<Self::ReturnType, SignatureVerificationError>
    where
        K: JsonWebKey<SigningAlgorithm = JS>;

    fn signing_alg(&self) -> Result<&JS, SignatureVerificationError> {
        match self.unverified_header().alg {
            JsonWebTokenAlgorithm::Signature(ref signing_alg) => Ok(signing_alg),
            JsonWebTokenAlgorithm::Encryption(ref other) => {
                Err(SignatureVerificationError::UnsupportedAlg(
                    serde_plain::to_string(other).unwrap_or_else(|err| {
                        panic!("encryption alg {other:?} failed to serialize to a string: {err}")
                    }),
                ))
            }
            // Section 2 of OpenID Connect Core 1.0 specifies that "ID Tokens MUST NOT use
            // none as the alg value unless the Response Type used returns no ID Token from
            // the Authorization Endpoint (such as when using the Authorization Code Flow)
            // and the Client explicitly requested the use of none at Registration time."
            //
            // While there's technically a use case where this is ok, we choose not to
            // support it for now to protect against accidental misuse. If demand arises,
            // we can figure out a API that mitigates the risk.
            JsonWebTokenAlgorithm::None => Err(SignatureVerificationError::NoSignature),
        }
    }
}

/// Error creating a JSON Web Token.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JsonWebTokenError {
    /// Failed to serialize JWT.
    #[error("Failed to serialize JWT")]
    SerializationError(#[source] serde_json::Error),
    /// Failed to sign JWT.
    #[error("Failed to sign JWT")]
    SigningError(#[source] SigningError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonWebToken<JE, JS, P, S>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
    S: JsonWebTokenPayloadSerde<P>,
{
    header: JsonWebTokenHeader<JE, JS>,
    payload: P,
    signature: Vec<u8>,
    signing_input: String,
    _phantom: PhantomData<S>,
}
impl<JE, JS, P, S> JsonWebToken<JE, JS, P, S>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
    S: JsonWebTokenPayloadSerde<P>,
{
    pub fn new<SK>(payload: P, signing_key: &SK, alg: &JS) -> Result<Self, JsonWebTokenError>
    where
        SK: PrivateSigningKey,
        <SK as PrivateSigningKey>::VerificationKey: JsonWebKey<SigningAlgorithm = JS>,
    {
        let header = JsonWebTokenHeader::<JE, _> {
            alg: JsonWebTokenAlgorithm::Signature(alg.clone()),
            crit: None,
            cty: None,
            kid: signing_key.as_verification_key().key_id().cloned(),
            typ: None,
        };

        let header_json =
            serde_json::to_string(&header).map_err(JsonWebTokenError::SerializationError)?;
        let header_base64 = BASE64_URL_SAFE_NO_PAD.encode(header_json);

        let serialized_payload =
            S::serialize(&payload).map_err(JsonWebTokenError::SerializationError)?;
        let payload_base64 = BASE64_URL_SAFE_NO_PAD.encode(serialized_payload);

        let signing_input = format!("{header_base64}.{payload_base64}");

        let signature = signing_key
            .sign(alg, signing_input.as_bytes())
            .map_err(JsonWebTokenError::SigningError)?;

        Ok(JsonWebToken {
            header,
            payload,
            signature,
            signing_input,
            _phantom: PhantomData,
        })
    }
}
// Owned JWT.
impl<JE, JS, P, S> JsonWebTokenAccess<JE, JS, P> for JsonWebToken<JE, JS, P, S>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
    S: JsonWebTokenPayloadSerde<P>,
{
    type ReturnType = P;
    fn unverified_header(&self) -> &JsonWebTokenHeader<JE, JS> {
        &self.header
    }
    fn unverified_payload(self) -> Self::ReturnType {
        self.payload
    }
    fn unverified_payload_ref(&self) -> &P {
        &self.payload
    }
    fn payload<K>(
        self,
        signature_alg: &JS,
        key: &K,
    ) -> Result<Self::ReturnType, SignatureVerificationError>
    where
        K: JsonWebKey<SigningAlgorithm = JS>,
    {
        key.verify_signature(
            signature_alg,
            self.signing_input.as_bytes(),
            &self.signature,
        )?;
        Ok(self.payload)
    }
}
// Borrowed JWT.
impl<'a, JE, JS, P, S> JsonWebTokenAccess<JE, JS, P> for &'a JsonWebToken<JE, JS, P, S>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
    S: JsonWebTokenPayloadSerde<P>,
{
    type ReturnType = &'a P;
    fn unverified_header(&self) -> &JsonWebTokenHeader<JE, JS> {
        &self.header
    }
    fn unverified_payload(self) -> Self::ReturnType {
        &self.payload
    }
    fn unverified_payload_ref(&self) -> &P {
        &self.payload
    }
    fn payload<K>(
        self,
        signature_alg: &JS,
        key: &K,
    ) -> Result<Self::ReturnType, SignatureVerificationError>
    where
        K: JsonWebKey<SigningAlgorithm = JS>,
    {
        key.verify_signature(
            signature_alg,
            self.signing_input.as_bytes(),
            &self.signature,
        )?;
        Ok(&self.payload)
    }
}
impl<'de, JE, JS, P, S> Deserialize<'de> for JsonWebToken<JE, JS, P, S>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
    S: JsonWebTokenPayloadSerde<P>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct JsonWebTokenVisitor<
            JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
            JS: JwsSigningAlgorithm,
            P: Debug + DeserializeOwned + Serialize,
            S: JsonWebTokenPayloadSerde<P>,
        >(PhantomData<(JE, JS, P, S)>);
        impl<'de, JE, JS, P, S> Visitor<'de> for JsonWebTokenVisitor<JE, JS, P, S>
        where
            JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
            JS: JwsSigningAlgorithm,
            P: Debug + DeserializeOwned + Serialize,
            S: JsonWebTokenPayloadSerde<P>,
        {
            type Value = JsonWebToken<JE, JS, P, S>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("JsonWebToken")
            }

            fn visit_str<DE>(self, v: &str) -> Result<Self::Value, DE>
            where
                DE: serde::de::Error,
            {
                let raw_token = v.to_string();
                let header: JsonWebTokenHeader<JE, JS>;
                let payload: P;
                let signature;
                let signing_input;

                {
                    let parts = raw_token.split('.').collect::<Vec<_>>();

                    // NB: We avoid including the full payload encoding in the error output to avoid
                    // clients potentially logging sensitive values.
                    if parts.len() != 3 {
                        return Err(DE::custom(format!(
                            "Invalid JSON web token: found {} parts (expected 3)",
                            parts.len()
                        )));
                    }

                    let header_json = crate::core::base64_url_safe_no_pad()
                        .decode(parts[0])
                        .map_err(|err| {
                            DE::custom(format!("Invalid base64url header encoding: {err:?}"))
                        })?;
                    header = serde_json::from_slice(&header_json).map_err(|err| {
                        DE::custom(format!("Failed to parse header JSON: {err:?}"))
                    })?;

                    let raw_payload = crate::core::base64_url_safe_no_pad()
                        .decode(parts[1])
                        .map_err(|err| {
                            DE::custom(format!("Invalid base64url payload encoding: {err:?}"))
                        })?;
                    payload = S::deserialize::<DE>(&raw_payload)?;

                    signature = crate::core::base64_url_safe_no_pad()
                        .decode(parts[2])
                        .map_err(|err| {
                            DE::custom(format!("Invalid base64url signature encoding: {err:?}"))
                        })?;

                    signing_input = format!("{}.{}", parts[0], parts[1]);
                }

                Ok(JsonWebToken {
                    header,
                    payload,
                    signature,
                    signing_input,
                    _phantom: PhantomData,
                })
            }
        }
        deserializer.deserialize_str(JsonWebTokenVisitor(PhantomData))
    }
}
impl<JE, JS, P, S> Serialize for JsonWebToken<JE, JS, P, S>
where
    JE: JweContentEncryptionAlgorithm<KeyType = JS::KeyType>,
    JS: JwsSigningAlgorithm,
    P: Debug + DeserializeOwned + Serialize,
    S: JsonWebTokenPayloadSerde<P>,
{
    fn serialize<SE>(&self, serializer: SE) -> Result<SE::Ok, SE::Error>
    where
        SE: Serializer,
    {
        let signature_base64 = BASE64_URL_SAFE_NO_PAD.encode(&self.signature);
        serializer.serialize_str(&format!("{}.{}", self.signing_input, signature_base64))
    }
}
