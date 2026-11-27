use crate::{SignatureVerificationError, SigningError};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use std::fmt::Debug;
use std::hash::Hash;

new_type![
    /// ID of a JSON Web Key.
    #[derive(Deserialize, Hash, Ord, PartialOrd, Serialize)]
    JsonWebKeyId(String)
];

/// JSON Web Key.
pub trait JsonWebKey: Clone + Debug + DeserializeOwned + Serialize + 'static {
    /// Allowed key usage.
    type KeyUse: JsonWebKeyUse;

    /// JSON Web Signature (JWS) algorithm.
    type SigningAlgorithm: JwsSigningAlgorithm;

    /// Returns the key ID, or `None` if no key ID is specified.
    fn key_id(&self) -> Option<&JsonWebKeyId>;

    /// Returns the key type (e.g., RSA).
    fn key_type(&self) -> &<Self::SigningAlgorithm as JwsSigningAlgorithm>::KeyType;

    /// Returns the allowed key usage (e.g., signing or encryption), or `None` if no usage is
    /// specified.
    fn key_use(&self) -> Option<&Self::KeyUse>;

    /// Returns the algorithm (e.g. ES512) this key must be used with, or `Unspecified` if
    /// no algorithm constraint was given, or unsupported if the algorithm is not for signing.
    ///
    /// It's not sufficient to tell whether a key can be used for signing, as key use also has to be validated.
    fn signing_alg(&self) -> JsonWebKeyAlgorithm<&Self::SigningAlgorithm>;

    /// Initializes a new symmetric key or shared signing secret from the specified raw bytes.
    fn new_symmetric(key: Vec<u8>) -> Self;

    /// Verifies the given `signature` using the given signature algorithm (`signature_alg`) over
    /// the given `message`.
    ///
    /// Returns `Ok` if the signature is valid, or an `Err` otherwise.
    fn verify_signature(
        &self,
        signature_alg: &Self::SigningAlgorithm,
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), SignatureVerificationError>;

    /// Hashes the given `bytes` using the hash function associated with the specified signing
    /// algorithm and returns the hashed bytes.
    ///
    /// Certain signing algorithms (e.g., `EdDSA`) use different hash functions depending on the
    /// type of key (e.g., whether the `Ed25519` or `Ed448` curve is used), so this method is
    /// implemented on the corresponding public key instead of the [`JwsSigningAlgorithm`] trait
    /// to allow the implementation to determine the proper hash function to use.
    /// If hashing fails or this key/signing algorithm does not have an associated hash function, an
    /// `Err` is returned with a string describing the cause of the error. An error is also returned
    /// if the specified signature algorithm is incompatible with this key (e.g., passing `EdDSA`
    /// with an RSA key).
    fn hash_bytes(&self, bytes: &[u8], alg: &Self::SigningAlgorithm) -> Result<Vec<u8>, String>;
}

/// Encodes a JWK key's alg field compatibility with either signing or encryption operations.
#[derive(Debug)]
pub enum JsonWebKeyAlgorithm<A: Debug> {
    /// the alg field allows this kind of operation to be performed with this algorithm only
    Algorithm(A),
    /// there is no alg field
    Unspecified,
    /// the alg field's algorithm is incompatible with this kind of operation
    Unsupported,
}

/// Private or symmetric key for signing.
pub trait PrivateSigningKey {
    /// Corresponding type of JSON Web Key used for verifying signatures produced by this key.
    type VerificationKey: JsonWebKey;

    /// Signs the given `message` using the given signature algorithm.
    fn sign(
        &self,
        signature_alg: &<Self::VerificationKey as JsonWebKey>::SigningAlgorithm,
        message: &[u8],
    ) -> Result<Vec<u8>, SigningError>;

    /// Converts this key to a JSON Web Key that can be used for verifying signatures.
    fn as_verification_key(&self) -> Self::VerificationKey;
}

/// Key type (e.g., RSA).
pub trait JsonWebKeyType:
    Clone + Debug + DeserializeOwned + PartialEq + Serialize + 'static
{
}

/// Allowed key usage.
pub trait JsonWebKeyUse: Debug + DeserializeOwned + Serialize + 'static {
    /// Returns true if the associated key may be used for digital signatures, or false otherwise.
    fn allows_signature(&self) -> bool;

    /// Returns true if the associated key may be used for encryption, or false otherwise.
    fn allows_encryption(&self) -> bool;
}

/// JSON Web Encryption (JWE) content encryption algorithm.
pub trait JweContentEncryptionAlgorithm:
    Clone + Debug + DeserializeOwned + Serialize + 'static
{
    /// Key type (e.g., RSA).
    type KeyType: JsonWebKeyType;

    /// Returns the type of key required to use this encryption algorithm.
    fn key_type(&self) -> Result<Self::KeyType, String>;
}

/// JSON Web Encryption (JWE) key management algorithm.
pub trait JweKeyManagementAlgorithm: Debug + DeserializeOwned + Serialize + 'static {
    // TODO: add a key_type() method
}

/// JSON Web Signature (JWS) algorithm.
pub trait JwsSigningAlgorithm:
    Clone + Debug + DeserializeOwned + Eq + Hash + PartialEq + Serialize + 'static
{
    /// Key type (e.g., RSA).
    type KeyType: JsonWebKeyType;

    /// Returns the type of key required to use this signature algorithm, or `None` if this
    /// algorithm does not require a key.
    fn key_type(&self) -> Option<Self::KeyType>;

    /// Returns true if the signature algorithm uses a shared secret (symmetric key).
    fn uses_shared_secret(&self) -> bool;

    /// Returns the RS256 algorithm.
    ///
    /// This is the default algorithm for OpenID Connect ID tokens and must be supported by all
    /// implementations.
    fn rsa_sha_256() -> Self;
}
