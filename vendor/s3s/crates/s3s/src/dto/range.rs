//! HTTP Range header

use crate::S3Error;
use crate::S3ErrorCode;
use crate::http;

use std::ops;

use atoi::FromRadix10Checked;
use stdx::str::StrExt;

/// HTTP Range header
///
/// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
///
/// See <https://www.rfc-editor.org/rfc/rfc9110.html#section-14.1.2>
#[allow(clippy::exhaustive_enums)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Range {
    /// Int range in bytes. This range is **inclusive**.
    ///
    /// See <https://www.rfc-editor.org/rfc/rfc9110.html#rule.int-range>
    Int {
        /// first position
        first: u64,
        /// last position
        last: Option<u64>,
    },
    /// Suffix range in bytes.
    ///
    /// See <https://www.rfc-editor.org/rfc/rfc9110.html#rule.suffix-range>
    Suffix {
        /// suffix length
        length: u64,
    },
}

/// [`Range`]
#[derive(Debug, thiserror::Error)]
#[error("ParseRangeError")]
pub struct ParseRangeError {
    /// private place holder
    _priv: (),
}

/// [`Range`]
#[derive(Debug, thiserror::Error)]
#[error("RangeNotSatisfiable")]
pub struct RangeNotSatisfiable {
    /// private place holder
    _priv: (),
}

impl Range {
    /// Parses `Range` from header
    /// # Errors
    /// Returns an error if the header is invalid
    pub fn parse(header: &str) -> Result<Self, ParseRangeError> {
        let err = || ParseRangeError { _priv: () };
        let s = header.strip_prefix("bytes=").ok_or_else(err)?.as_bytes();

        if let [b'-', s @ ..] = s {
            // suffix range
            let length = parse_u64_full(s).ok_or_else(err)?;
            return Ok(Range::Suffix { length });
        }

        // int range
        let (first, s) = parse_u64_once(s).ok_or_else(err)?;
        if first > (i64::MAX as u64) {
            return Err(err());
        }

        let [b'-', s @ ..] = s else { return Err(err()) };

        if s.is_empty() {
            // int range from
            return Ok(Range::Int { first, last: None });
        }

        // int range inclusive
        let last = parse_u64_full(s).ok_or_else(err)?;
        if last > (i64::MAX as u64) {
            return Err(err());
        }

        if first > last {
            return Err(err());
        }

        Ok(Range::Int { first, last: Some(last) })
    }

    #[must_use]
    pub fn to_header_string(&self) -> String {
        match self {
            Range::Int { first, last } => match last {
                Some(last) => format!("bytes={first}-{last}"),
                None => format!("bytes={first}-"),
            },
            Range::Suffix { length } => format!("bytes=-{length}"),
        }
    }

    /// Checks if the range is satisfiable
    ///  according to [RFC9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-byte-ranges).
    /// # Errors
    /// Returns an error if the range is not satisfiable
    #[allow(clippy::range_plus_one)] // cannot be fixed
    pub fn check(&self, full_length: u64) -> Result<ops::Range<u64>, RangeNotSatisfiable> {
        let err = || RangeNotSatisfiable { _priv: () };
        match *self {
            Range::Int { first, last } => {
                if first >= full_length {
                    return Err(err());
                }
                // 0 <= first < full_length

                match last {
                    Some(last) => {
                        let last = last.min(full_length - 1);
                        if first > last {
                            return Err(err());
                        }
                        // 0 <= first <= last < full_length
                        Ok(first..last + 1)
                    }
                    // 0 <= first < full_length
                    None => Ok(first..full_length),
                }
            }
            Range::Suffix { length } => {
                if length == 0 {
                    return Err(err());
                }
                let length = length.min(full_length);
                Ok((full_length - length)..full_length)
            }
        }
    }
}

impl From<RangeNotSatisfiable> for S3Error {
    #[inline]
    fn from(_: RangeNotSatisfiable) -> Self {
        S3ErrorCode::InvalidRange.into()
    }
}

impl http::TryFromHeaderValue for Range {
    type Error = ParseRangeError;

    fn try_from_header_value(val: &http::HeaderValue) -> Result<Self, Self::Error> {
        let header = str::from_ascii_simd(val.as_bytes()).map_err(|_| ParseRangeError { _priv: () })?;
        Self::parse(header)
    }
}

fn parse_u64_full(s: &[u8]) -> Option<u64> {
    match u64::from_radix_10_checked(s) {
        (Some(x), pos) if pos == s.len() => Some(x),
        _ => None,
    }
}

fn parse_u64_once(s: &[u8]) -> Option<(u64, &[u8])> {
    match u64::from_radix_10_checked(s) {
        (Some(x), pos) if pos > 0 => Some((x, &s[pos..])),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range_int_inclusive(first: u64, last: u64) -> Range {
        Range::Int { first, last: Some(last) }
    }

    fn range_int_from(first: u64) -> Range {
        Range::Int { first, last: None }
    }

    fn range_suffix(length: u64) -> Range {
        Range::Suffix { length }
    }

    #[test]
    fn byte_range() {
        let cases = [
            ("bytes=0-499", Ok(range_int_inclusive(0, 499))),
            ("bytes=0-499;", Err(())),
            ("bytes=9500-", Ok(range_int_from(9500))),
            ("bytes=9500-0-", Err(())),
            ("bytes=9500", Err(())),
            ("bytes=0-0", Ok(range_int_inclusive(0, 0))),
            ("bytes=-500", Ok(range_suffix(500))),
            ("bytes=-500 ", Err(())),
            ("bytes=-+500", Err(())),
            ("bytes=-1000000000000000000000000", Err(())),
            ("bytes=-0", Ok(range_suffix(0))),
            ("bytes=-000", Ok(range_suffix(0))),
        ];

        for (input, expected) in &cases {
            let output = Range::parse(input);
            match expected {
                Ok(expected) => assert_eq!(output.unwrap(), *expected),
                Err(()) => assert!(output.is_err()),
            }
        }
    }

    #[test]
    fn satisfiable() {
        let cases = [
            (10000, range_int_from(9500), Ok(9500..10000)),
            (10000, range_int_from(10000), Err(())),
            (10000, range_int_inclusive(0, 499), Ok(0..500)),
            (10000, range_int_inclusive(0, 0), Ok(0..1)),
            (10000, range_int_inclusive(9500, 50000), Ok(9500..10000)),
            (10000, range_int_inclusive(10000, 10000), Err(())),
            (10000, range_suffix(500), Ok(9500..10000)),
            (10000, range_suffix(10000), Ok(0..10000)),
            (10000, range_suffix(0), Err(())),
            (0, range_int_from(0), Err(())),
            (0, range_suffix(1), Ok(0..0)),
            (0, range_suffix(0), Err(())),
        ];

        for &(full_length, ref range, ref expected) in &cases {
            let output = range.check(full_length);
            match expected {
                Ok(expected) => assert_eq!(output.unwrap(), *expected),
                Err(()) => assert!(output.is_err(), "{full_length:?}, {range:?}"),
            }
        }
    }

    #[test]
    fn to_header_string_int_inclusive() {
        let range = range_int_inclusive(0, 499);
        assert_eq!(range.to_header_string(), "bytes=0-499");
    }

    #[test]
    fn to_header_string_int_from() {
        let range = range_int_from(9500);
        assert_eq!(range.to_header_string(), "bytes=9500-");
    }

    #[test]
    fn to_header_string_suffix() {
        let range = range_suffix(500);
        assert_eq!(range.to_header_string(), "bytes=-500");
    }

    #[test]
    fn to_header_string_roundtrip() {
        let cases = [range_int_inclusive(0, 499), range_int_from(9500), range_suffix(500)];
        for range in &cases {
            let header = range.to_header_string();
            let parsed = Range::parse(&header).unwrap();
            assert_eq!(*range, parsed);
        }
    }

    #[test]
    fn try_from_header_value() {
        use crate::http::TryFromHeaderValue;
        let hv = http::HeaderValue::from_static("bytes=0-499");
        let range = Range::try_from_header_value(&hv).unwrap();
        assert_eq!(range, range_int_inclusive(0, 499));

        let hv = http::HeaderValue::from_static("bytes=100-");
        let range = Range::try_from_header_value(&hv).unwrap();
        assert_eq!(range, range_int_from(100));

        let hv = http::HeaderValue::from_static("bytes=-500");
        let range = Range::try_from_header_value(&hv).unwrap();
        assert_eq!(range, range_suffix(500));
    }

    #[test]
    fn range_not_satisfiable_to_s3_error() {
        let err = RangeNotSatisfiable { _priv: () };
        let s3_err: S3Error = err.into();
        let code = s3_err.code();
        assert_eq!(code, &S3ErrorCode::InvalidRange);
    }

    #[test]
    fn parse_first_exceeds_i64_max() {
        let big = format!("bytes={}-", i64::MAX as u64 + 1);
        assert!(Range::parse(&big).is_err());
    }

    #[test]
    fn parse_last_exceeds_i64_max() {
        let big = format!("bytes=0-{}", i64::MAX as u64 + 1);
        assert!(Range::parse(&big).is_err());
    }

    #[test]
    fn parse_first_greater_than_last() {
        assert!(Range::parse("bytes=500-100").is_err());
    }
}
