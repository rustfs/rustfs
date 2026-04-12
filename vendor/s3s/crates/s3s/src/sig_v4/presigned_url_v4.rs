//! presigned url information

use super::AmzDate;
use super::CredentialV4;

use crate::http::OrderedQs;
use crate::utils::crypto::is_sha256_checksum;

use smallvec::SmallVec;

/// Presigned url information
#[derive(Debug)]
pub struct PresignedUrlV4<'a> {
    /// algorithm
    pub algorithm: &'a str,
    /// credential
    pub credential: CredentialV4<'a>,
    /// amz date
    pub amz_date: AmzDate,
    /// expires
    pub expires: time::Duration,
    /// signed headers
    pub signed_headers: SmallVec<[&'a str; 16]>,
    /// signature
    pub signature: &'a str,
}

/// [`PresignedUrlV4`]
#[derive(Debug, thiserror::Error)]
#[error("ParsePresignedUrlError")]
pub struct ParsePresignedUrlError {
    /// priv place holder
    _priv: (),
}

/// query strings of a presigned url
struct PresignedQs<'a> {
    /// X-Amz-Algorithm
    algorithm: &'a str,
    /// X-Amz-Credential
    credential: &'a str,
    /// X-Amz-Date
    date: &'a str,
    /// X-Amz-Expires
    expires: &'a str,
    /// X-Amz-SignedHeaders
    signed_headers: &'a str,
    /// X-Amz-Signature
    signature: &'a str,
}

impl<'a> PresignedQs<'a> {
    /// Creates `PresignedQs` from `OrderedQs`
    fn from_ordered_qs(qs: &'a OrderedQs) -> Option<Self> {
        Some(PresignedQs {
            algorithm: qs.get_unique("X-Amz-Algorithm")?,
            credential: qs.get_unique("X-Amz-Credential")?,
            date: qs.get_unique("X-Amz-Date")?,
            expires: qs.get_unique("X-Amz-Expires")?,
            signed_headers: qs.get_unique("X-Amz-SignedHeaders")?,
            signature: qs.get_unique("X-Amz-Signature")?,
        })
    }
}

impl<'a> PresignedUrlV4<'a> {
    /// Parses `PresignedUrl` from query
    ///
    /// # Errors
    /// Returns `ParsePresignedUrlError` if it failed to parse
    pub fn parse(qs: &'a OrderedQs) -> Result<Self, ParsePresignedUrlError> {
        let err = || ParsePresignedUrlError { _priv: () };

        let info = PresignedQs::from_ordered_qs(qs).ok_or_else(err)?;

        let algorithm = info.algorithm;

        let credential = CredentialV4::parse(info.credential).map_err(|_e| err())?;

        let amz_date = AmzDate::parse(info.date).map_err(|_e| err())?;

        let expires = parse_expires(info.expires).ok_or_else(err)?;

        if !info.signed_headers.is_ascii() {
            return Err(err());
        }
        let signed_headers = info.signed_headers.split(';').collect();

        if !is_sha256_checksum(info.signature) {
            return Err(err());
        }
        let signature = info.signature;

        Ok(Self {
            algorithm,
            credential,
            amz_date,
            expires,
            signed_headers,
            signature,
        })
    }
}

fn parse_expires(s: &str) -> Option<time::Duration> {
    let x = s.parse::<u32>().ok()?;
    Some(time::Duration::new(i64::from(x), 0))
}
