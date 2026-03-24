use crate::http::OrderedQs;

use std::borrow::Cow;

use time::OffsetDateTime;

pub struct PresignedUrlV2<'a> {
    pub access_key: &'a str,
    pub expires_time: OffsetDateTime,
    pub signature: Cow<'a, str>,
}

/// [`PresignedUrlV2`]
#[derive(Debug, thiserror::Error)]
#[error("ParsePresignedUrlError")]
pub struct ParsePresignedUrlError {
    /// priv place holder
    _priv: (),
}

impl<'a> PresignedUrlV2<'a> {
    pub fn parse(qs: &'a OrderedQs) -> Result<Self, ParsePresignedUrlError> {
        let err = || ParsePresignedUrlError { _priv: () };

        let access_key = qs.get_unique("AWSAccessKeyId").ok_or_else(err)?;
        let expires_str = qs.get_unique("Expires").ok_or_else(err)?;
        let signature = qs.get_unique("Signature").ok_or_else(err)?;

        let expires_time = parse_unix_timestamp(expires_str).ok_or_else(err)?;
        let signature = urlencoding::decode(signature).map_err(|_| err())?;

        Ok(Self {
            access_key,
            expires_time,
            signature,
        })
    }
}

fn parse_unix_timestamp(s: &str) -> Option<OffsetDateTime> {
    let ts = s.parse::<i64>().ok().filter(|&x| x >= 0)?;
    OffsetDateTime::from_unix_timestamp(ts).ok()
}
