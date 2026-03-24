use std::str::FromStr;

use http::HeaderValue;
use http::header::InvalidHeaderValue;

use super::etag::{ETag, ParseETagError};

/// Condition value for `If-Match`, `If-None-Match` and related headers.
///
/// According to RFC 9110, these headers can contain either:
/// - An `ETag` value (strong or weak): `"value"` or `W/"value"`
/// - A wildcard: `*` (matches any existing entity)
///
/// The wildcard is commonly used for conditional requests like:
/// - `If-None-Match: *` - Only create if the resource doesn't exist (PUT)
/// - `If-Match: *` - Only modify if the resource exists
///
/// See RFC 9110 §13.1 and MDN:
/// + <https://www.rfc-editor.org/rfc/rfc9110#section-13.1>
/// + <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ETagCondition {
    /// An `ETag` value (strong or weak)
    ETag(ETag),
    /// The wildcard `*` that matches any existing entity
    Any,
}

/// Errors returned when parsing an `ETagCondition` header.
#[derive(Debug, thiserror::Error)]
pub enum ParseETagConditionError {
    /// The bytes do not match the expected syntax.
    #[error("ParseETagConditionError: InvalidFormat")]
    InvalidFormat,
    /// Contains invalid characters.
    #[error("ParseETagConditionError: InvalidChar")]
    InvalidChar,
    /// Error parsing the `ETag` value
    #[error("ParseETagConditionError: {0}")]
    ETagError(#[from] ParseETagError),
}

impl ETagCondition {
    /// Parses an `ETagCondition` from header bytes.
    ///
    /// # Errors
    /// + Returns `ParseETagConditionError::InvalidFormat` if the bytes do not match the expected syntax.
    /// + Returns `ParseETagConditionError::InvalidChar` if the value contains invalid characters
    pub fn parse_http_header(src: &[u8]) -> Result<Self, ParseETagConditionError> {
        // Check for wildcard
        if src == b"*" {
            return Ok(ETagCondition::Any);
        }

        // Otherwise, parse as ETag
        let etag = ETag::parse_http_header(src)?;
        Ok(ETagCondition::ETag(etag))
    }

    /// Encodes this `ETagCondition` as an HTTP header value.
    ///
    /// # Errors
    /// Returns `InvalidHeaderValue` if the `ETag` value contains invalid characters for HTTP headers.
    pub fn to_http_header(&self) -> Result<HeaderValue, InvalidHeaderValue> {
        match self {
            ETagCondition::ETag(etag) => etag.to_http_header(),
            ETagCondition::Any => HeaderValue::try_from("*"),
        }
    }

    /// Returns the `ETag` if this is an `ETagCondition::ETag`, otherwise `None`.
    #[must_use]
    pub fn as_etag(&self) -> Option<&ETag> {
        match self {
            ETagCondition::ETag(etag) => Some(etag),
            ETagCondition::Any => None,
        }
    }

    /// Consumes self and returns the `ETag` if this is an `ETagCondition::ETag`, otherwise `None`.
    #[must_use]
    pub fn into_etag(self) -> Option<ETag> {
        match self {
            ETagCondition::ETag(etag) => Some(etag),
            ETagCondition::Any => None,
        }
    }

    /// Returns true if this is the wildcard `*`.
    #[must_use]
    pub fn is_any(&self) -> bool {
        matches!(self, ETagCondition::Any)
    }
}

impl FromStr for ETagCondition {
    type Err = ParseETagConditionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_http_header(s.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::{ETag, ETagCondition, ParseETagConditionError};

    #[test]
    fn parse_wildcard() {
        let cond = ETagCondition::parse_http_header(b"*").expect("parse wildcard");
        assert!(cond.is_any());
        assert_eq!(cond.as_etag(), None);
    }

    #[test]
    fn parse_strong_etag() {
        let cond = ETagCondition::parse_http_header(b"\"abc123\"").expect("parse strong etag");
        assert!(!cond.is_any());
        let etag = cond.as_etag().expect("should be etag");
        assert_eq!(etag.as_strong(), Some("abc123"));
    }

    #[test]
    fn parse_weak_etag() {
        let cond = ETagCondition::parse_http_header(b"W/\"xyz\"").expect("parse weak etag");
        assert!(!cond.is_any());
        let etag = cond.as_etag().expect("should be etag");
        assert_eq!(etag.as_weak(), Some("xyz"));
    }

    #[test]
    fn to_header_wildcard() {
        let cond = ETagCondition::Any;
        let hv = cond.to_http_header().expect("wildcard header");
        assert_eq!(hv.as_bytes(), b"*");
    }

    #[test]
    fn to_header_strong_etag() {
        let cond = ETagCondition::ETag(ETag::Strong("abc123".to_string()));
        let hv = cond.to_http_header().expect("strong etag header");
        assert_eq!(hv.as_bytes(), b"\"abc123\"");
    }

    #[test]
    fn to_header_weak_etag() {
        let cond = ETagCondition::ETag(ETag::Weak("xyz".to_string()));
        let hv = cond.to_http_header().expect("weak etag header");
        assert_eq!(hv.as_bytes(), b"W/\"xyz\"");
    }

    #[test]
    fn parse_and_header_roundtrip() {
        let cases = [("*", true), ("\"abc\"", false), ("W/\"xyz\"", false)];
        for (input, is_any) in cases {
            let cond = ETagCondition::parse_http_header(input.as_bytes()).expect("parse");
            assert_eq!(cond.is_any(), is_any);
            let hv = cond.to_http_header().expect("to header");
            let parsed_back = ETagCondition::parse_http_header(hv.as_bytes()).expect("parse back");
            assert_eq!(cond, parsed_back);
        }
    }

    #[test]
    fn from_str_trait() {
        let cond: ETagCondition = "*".parse().expect("parse wildcard from str");
        assert!(cond.is_any());

        let cond: ETagCondition = "\"abc123\"".parse().expect("parse strong from str");
        assert!(!cond.is_any());
        let etag = cond.as_etag().expect("should be etag");
        assert_eq!(etag.as_strong(), Some("abc123"));

        let cond: ETagCondition = "W/\"xyz\"".parse().expect("parse weak from str");
        assert!(!cond.is_any());
        let etag = cond.as_etag().expect("should be etag");
        assert_eq!(etag.as_weak(), Some("xyz"));
    }

    #[test]
    fn parse_invalid() {
        // Empty string should return error
        let err = ETagCondition::parse_http_header(b"").unwrap_err();
        assert!(matches!(
            err,
            ParseETagConditionError::InvalidFormat | ParseETagConditionError::ETagError(_)
        ));

        // Malformed values should return error
        let err = ETagCondition::parse_http_header(b"**").unwrap_err();
        assert!(matches!(
            err,
            ParseETagConditionError::InvalidFormat | ParseETagConditionError::ETagError(_)
        ));

        let err = ETagCondition::parse_http_header(b"* ").unwrap_err();
        assert!(matches!(
            err,
            ParseETagConditionError::InvalidFormat | ParseETagConditionError::ETagError(_)
        ));

        let err = ETagCondition::parse_http_header(b"\"unclosed").unwrap_err();
        assert!(matches!(
            err,
            ParseETagConditionError::InvalidFormat | ParseETagConditionError::ETagError(_)
        ));
    }

    #[test]
    fn parse_unquoted_values() {
        // Typical S3 ETag values without quotes (alphanumeric only)
        let cond = ETagCondition::parse_http_header(b"ABCORZ").expect("parse simple string");
        assert_eq!(cond.as_etag().unwrap().as_strong(), Some("ABCORZ"));

        let cond = ETagCondition::parse_http_header(b"4fcec74691ff529f6d016ec3629ff11b").expect("parse md5 hash");
        assert_eq!(cond.as_etag().unwrap().as_strong(), Some("4fcec74691ff529f6d016ec3629ff11b"));

        // Multipart upload ETag format
        let cond = ETagCondition::parse_http_header(b"4fcec74691ff529f6d016ec3629ff11b-5").expect("parse multipart etag");
        assert_eq!(cond.as_etag().unwrap().as_strong(), Some("4fcec74691ff529f6d016ec3629ff11b-5"));

        // Single-character alphanumeric should be parsed as ETag, not confused with wildcard "*"
        let cond = ETagCondition::parse_http_header(b"a").expect("parse single char");
        assert!(!cond.is_any()); // Should NOT be wildcard
        assert_eq!(cond.as_etag().unwrap().as_strong(), Some("a"));

        let cond = ETagCondition::parse_http_header(b"1").expect("parse single digit");
        assert!(!cond.is_any()); // Should NOT be wildcard
        assert_eq!(cond.as_etag().unwrap().as_strong(), Some("1"));
    }
}
