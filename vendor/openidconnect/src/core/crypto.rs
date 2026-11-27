use crate::core::jwk::CoreJsonCurveType;
use crate::core::{CoreJsonWebKey, CoreJsonWebKeyType};
use crate::helpers::Base64UrlEncodedBytes;
use crate::{JsonWebKey, SignatureVerificationError};

use std::ops::Deref;

/// Decode a JWK unsigned component into `rsa` 0.10's integer type.
///
/// Leading zero octets are discarded, matching the previous `BigUint` decode.
fn jwk_uint(bytes: &[u8]) -> Result<rsa::BoxedUint, String> {
    let start = bytes
        .iter()
        .position(|byte| *byte != 0)
        .ok_or_else(|| "RSA integer component is zero".to_string())?;
    let magnitude = &bytes[start..];
    let bit_len = u32::try_from(magnitude.len())
        .ok()
        .and_then(|len| len.checked_mul(8))
        .ok_or_else(|| "RSA integer component is too large".to_string())?;
    // `BoxedUint` precision is limb-aligned. Round up so the magnitude fits.
    let precision = bit_len.div_ceil(64).saturating_mul(64).max(64);
    rsa::BoxedUint::from_be_slice(magnitude, precision).map_err(|err| err.to_string())
}

fn rsa_public_key(
    key: &CoreJsonWebKey,
) -> Result<(&Base64UrlEncodedBytes, &Base64UrlEncodedBytes), String> {
    if *key.key_type() != CoreJsonWebKeyType::RSA {
        Err("RSA key required".to_string())
    } else {
        let n = key
            .n
            .as_ref()
            .ok_or_else(|| "RSA modulus `n` is missing".to_string())?;
        let e = key
            .e
            .as_ref()
            .ok_or_else(|| "RSA exponent `e` is missing".to_string())?;
        Ok((n, e))
    }
}

fn ec_public_key(
    key: &CoreJsonWebKey,
) -> Result<
    (
        &Base64UrlEncodedBytes,
        &Base64UrlEncodedBytes,
        &CoreJsonCurveType,
    ),
    String,
> {
    if *key.key_type() != CoreJsonWebKeyType::EllipticCurve {
        Err("EC key required".to_string())
    } else {
        let x = key
            .x
            .as_ref()
            .ok_or_else(|| "EC `x` part is missing".to_string())?;
        let y = key
            .y
            .as_ref()
            .ok_or_else(|| "EC `y` part is missing".to_string())?;
        let crv = key
            .crv
            .as_ref()
            .ok_or_else(|| "EC `crv` part is missing".to_string())?;
        Ok((x, y, crv))
    }
}

fn ed_public_key(
    key: &CoreJsonWebKey,
) -> Result<(&Base64UrlEncodedBytes, &CoreJsonCurveType), String> {
    if *key.key_type() != CoreJsonWebKeyType::OctetKeyPair {
        Err("OKP key required".to_string())
    } else {
        let x = key
            .x
            .as_ref()
            .ok_or_else(|| "OKP `x` part is missing".to_string())?;
        let crv = key
            .crv
            .as_ref()
            .ok_or_else(|| "OKP `crv` part is missing".to_string())?;
        Ok((x, crv))
    }
}

pub fn verify_rsa_signature(
    key: &CoreJsonWebKey,
    padding: impl rsa::traits::SignatureScheme,
    msg: &[u8],
    signature: &[u8],
) -> Result<(), SignatureVerificationError> {
    let (n, e) = rsa_public_key(key).map_err(SignatureVerificationError::InvalidKey)?;
    // Decode `n` and `e` as unsigned big-endian integers so a leading 0x00
    // (RFC 7518 §6.3.1.1) does not change the modulus width.
    let n_int = jwk_uint(n.deref()).map_err(SignatureVerificationError::InvalidKey)?;
    let e_int = jwk_uint(e.deref()).map_err(SignatureVerificationError::InvalidKey)?;
    let public_key = rsa::PublicKey::new(n_int, e_int)
        .map_err(|e| SignatureVerificationError::InvalidKey(e.to_string()))?;

    public_key
        .verify(padding, msg, signature)
        .map_err(|_| SignatureVerificationError::CryptoError("bad signature".to_string()))
}

/// According to RFC5480, Section-2.2 implementations of Elliptic Curve Cryptography MUST support the uncompressed form.
/// The first octet of the octet string indicates whether the uncompressed or compressed form is used. For the uncompressed
/// form, the first octet has to be 0x04.
/// According to https://briansmith.org/rustdoc/ring/signature/index.html#ecdsa__fixed-details-fixed-length-pkcs11-style-ecdsa-signatures,
/// to recover the X and Y coordinates from an octet string, the Octet-String-To-Elliptic-Curve-Point Conversion
/// is used (Section 2.3.4 of https://www.secg.org/sec1-v2.pdf).
pub fn verify_ec_signature(
    key: &CoreJsonWebKey,
    msg: &[u8],
    signature: &[u8],
) -> Result<(), SignatureVerificationError> {
    use p256::ecdsa::signature::Verifier;

    let (x, y, crv) = ec_public_key(key).map_err(SignatureVerificationError::InvalidKey)?;
    let mut pk = vec![0x04];
    pk.extend(x.deref());
    pk.extend(y.deref());
    match *crv {
        CoreJsonCurveType::P256 => {
            let public_key = p256::ecdsa::VerifyingKey::from_sec1_bytes(&pk)
                .map_err(|e| SignatureVerificationError::InvalidKey(e.to_string()))?;
            public_key
                .verify(
                    msg,
                    &p256::ecdsa::Signature::from_slice(signature).map_err(|_| {
                        SignatureVerificationError::CryptoError("Invalid signature".to_string())
                    })?,
                )
                .map_err(|_| {
                    SignatureVerificationError::CryptoError("EC Signature was wrong".to_string())
                })
        }
        CoreJsonCurveType::P384 => {
            let public_key = p384::ecdsa::VerifyingKey::from_sec1_bytes(&pk)
                .map_err(|e| SignatureVerificationError::InvalidKey(e.to_string()))?;
            public_key
                .verify(
                    msg,
                    &p384::ecdsa::Signature::from_slice(signature).map_err(|_| {
                        SignatureVerificationError::CryptoError("Invalid signature".to_string())
                    })?,
                )
                .map_err(|_| {
                    SignatureVerificationError::CryptoError("EC Signature was wrong".to_string())
                })
        }
        CoreJsonCurveType::P521 => Err(SignatureVerificationError::UnsupportedAlg(
            "P521".to_string(),
        )),
        _ => Err(SignatureVerificationError::InvalidKey(format!(
            "unrecognized curve `{crv:?}`"
        ))),
    }
}

pub fn verify_ed_signature(
    key: &CoreJsonWebKey,
    msg: &[u8],
    signature: &[u8],
) -> Result<(), SignatureVerificationError> {
    use ed25519_dalek::Verifier;

    let (x, crv) = ed_public_key(key).map_err(SignatureVerificationError::InvalidKey)?;

    match *crv {
        CoreJsonCurveType::Ed25519 => {
            let public_key = ed25519_dalek::VerifyingKey::try_from(x.deref().as_slice())
                .map_err(|e| SignatureVerificationError::InvalidKey(e.to_string()))?;

            public_key
                .verify(
                    msg,
                    &ed25519_dalek::Signature::from_slice(signature).map_err(|_| {
                        SignatureVerificationError::CryptoError("invalid signature".to_string())
                    })?,
                )
                .map_err(|_| {
                    SignatureVerificationError::CryptoError("incorrect EdDSA signature".to_string())
                })
        }
        _ => Err(SignatureVerificationError::InvalidKey(format!(
            "unrecognized curve `{crv:?}`"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::core::crypto::verify_rsa_signature;
    use crate::core::CoreJsonWebKey;

    use base64::prelude::BASE64_URL_SAFE_NO_PAD;
    use base64::Engine;
    use sha2::Digest;

    #[test]
    fn test_leading_zeros_are_parsed_correctly() {
        // The message we signed
        let msg = "THIS IS A SIGNATURE TEST";
        let signature = BASE64_URL_SAFE_NO_PAD.decode("bg0ohqKwYHAiODeG6qkJ-6IhodN7LGPxAh4hbWeIoBdSXrXMt8Ft8U0BV7vANPvF56h20XB9C0021x2kt7iAbMgPNcZ7LCuXMPPq04DrBpMHafH5BXBwnyDKJKrzDm5sfr6OgEkcxSLHaSJ6gTWQ3waPt6_SeH2-Fi74rg13MHyX-0iqz7bZveoBbGIs5yQCwvXgrDS9zW5LUwUHozHfE6FuSi_Z92ioXeu7FHHDg1KFfg3hs8ZLx4wAX15Vw2GCQOzvyNdbItxXRLnrN1NPqxFquVNo5RGlx6ihR1Jfe7y_n0NSR2q2TuU4cIwR0LRwEaANy5SDqtleQPrTEn8nGQ").unwrap();
        // RSA pub key with leading 0
        let key : CoreJsonWebKey = serde_json::from_value(serde_json::json!(
            {
            "kty": "RSA",
            "e": "AQAB",
            "use": "sig",
            "kid": "TEST_KEY_ID",
            "alg": "RS256",
            "n": "AN0M6Y760b9Ok2PxDOps1TgSmiOaR9mLIfUHtZ_o-6JypOckGcl1CxrteyokOb3WyDsfIAN9fFNrycv5YoLKO7sh0IcfzNEXFgzK84HTBcGuqhN8NV98Z6N9EryUrgJYsJeVoPYm0MzkDe4NyWHhnq-9OyNCQzVELH0NhhViQqRyM92OPrJcQlk8s3ZvcgRmkd-rEtRua8SbS3GEvfvgweVy5-qcJCGoziKfx-IteMOm6yKoHvqisKb91N-qw_kSS4YQUx-DZVDo2g24F7VIbcYzJGUOU674HUF1j-wJyXzG3VV8lAXD8hABs5Lh87gr8_hIZD5gbYBJRObJk9XZbfk"
            }
        )).unwrap();

        let mut hasher = sha2::Sha256::new();
        hasher.update(msg);
        let hash = hasher.finalize().to_vec();
        assert! {
            verify_rsa_signature(
                &key,
                rsa::Pkcs1v15Sign::new::<rsa::sha2::Sha256>(),
                &hash,
                &signature,
            ).is_ok()
        }
    }
}
