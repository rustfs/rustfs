use super::de::TryFromHeaderValue;
use super::ser::TryIntoHeaderValue;

use crate::dto::ETag;
use crate::dto::ETagCondition;
use crate::dto::ParseETagConditionError;
use crate::dto::ParseETagError;

use http::HeaderValue;
use http::header::InvalidHeaderValue;

impl TryFromHeaderValue for ETag {
    type Error = ParseETagError;

    fn try_from_header_value(value: &HeaderValue) -> Result<Self, Self::Error> {
        Self::parse_http_header(value.as_bytes())
    }
}

impl TryIntoHeaderValue for ETag {
    type Error = InvalidHeaderValue;

    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error> {
        self.to_http_header()
    }
}

impl TryFromHeaderValue for ETagCondition {
    type Error = ParseETagConditionError;

    fn try_from_header_value(value: &HeaderValue) -> Result<Self, Self::Error> {
        Self::parse_http_header(value.as_bytes())
    }
}

impl TryIntoHeaderValue for ETagCondition {
    type Error = InvalidHeaderValue;

    fn try_into_header_value(self) -> Result<HeaderValue, Self::Error> {
        self.to_http_header()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dto::{ETag, ETagCondition};

    #[test]
    fn etag_try_from_header_value() {
        let hv = HeaderValue::from_static("\"abc123\"");
        let etag = ETag::try_from_header_value(&hv).unwrap();
        assert_eq!(etag.as_strong(), Some("abc123"));

        let hv = HeaderValue::from_static("W/\"weak\"");
        let etag = ETag::try_from_header_value(&hv).unwrap();
        assert_eq!(etag.as_weak(), Some("weak"));

        let hv = HeaderValue::from_static("abc123def");
        let etag = ETag::try_from_header_value(&hv).unwrap();
        assert_eq!(etag.as_strong(), Some("abc123def"));
    }

    #[test]
    fn etag_try_into_header_value() {
        let etag = ETag::Strong("hello".to_owned());
        let hv = etag.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"\"hello\"");

        let etag = ETag::Weak("world".to_owned());
        let hv = etag.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"W/\"world\"");
    }

    #[test]
    fn etag_condition_try_from_header_value() {
        let hv = HeaderValue::from_static("*");
        let cond = ETagCondition::try_from_header_value(&hv).unwrap();
        assert!(cond.is_any());

        let hv = HeaderValue::from_static("\"abc\"");
        let cond = ETagCondition::try_from_header_value(&hv).unwrap();
        assert_eq!(cond.as_etag().unwrap().as_strong(), Some("abc"));
    }

    #[test]
    fn etag_condition_try_into_header_value() {
        let cond = ETagCondition::Any;
        let hv = cond.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"*");

        let cond = ETagCondition::ETag(ETag::Strong("test".to_owned()));
        let hv = cond.try_into_header_value().unwrap();
        assert_eq!(hv.as_bytes(), b"\"test\"");
    }

    #[test]
    fn etag_roundtrip_via_header_value() {
        let original = ETag::Strong("roundtrip".to_owned());
        let hv = original.clone().try_into_header_value().unwrap();
        let parsed = ETag::try_from_header_value(&hv).unwrap();
        assert_eq!(original, parsed);

        let original = ETagCondition::ETag(ETag::Weak("rt".to_owned()));
        let hv = original.clone().try_into_header_value().unwrap();
        let parsed = ETagCondition::try_from_header_value(&hv).unwrap();
        assert_eq!(original, parsed);
    }
}
