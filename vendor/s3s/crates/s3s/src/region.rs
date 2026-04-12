//! S3 region handling
//!
//! This module provides a strongly-typed [`Region`] type that ensures the
//! region string conforms to the pattern `[a-z0-9-]+`.

use std::fmt;
use std::str::FromStr;

/// Error returned when a region string is invalid.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid region: {0:?}")]
pub struct InvalidRegion(Box<str>);

/// A validated S3 region name.
///
/// The inner string is guaranteed to match the pattern `[a-z0-9-]+`:
/// it is non-empty and contains only lowercase ASCII letters, digits,
/// and hyphens.
///
/// # Examples
///
/// ```
/// use s3s::region::Region;
///
/// let region: Region = "us-east-1".parse().unwrap();
/// assert_eq!(region.as_str(), "us-east-1");
///
/// let err = "US-EAST-1".parse::<Region>();
/// assert!(err.is_err());
///
/// let err = "".parse::<Region>();
/// assert!(err.is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Region(Box<str>);

impl Region {
    /// Validates a region string.
    ///
    /// Returns `true` if every byte matches `[a-z0-9-]` and the string
    /// is non-empty.
    fn is_valid(s: &str) -> bool {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }

    /// Creates a new `Region`, returning an error if the format is invalid.
    ///
    /// A valid region name must be non-empty and contain only characters
    /// matching `[a-z0-9-]`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidRegion`] if the string is empty or contains
    /// characters outside `[a-z0-9-]`.
    pub fn new(s: Box<str>) -> Result<Self, InvalidRegion> {
        if Self::is_valid(&s) {
            Ok(Self(s))
        } else {
            Err(InvalidRegion(s))
        }
    }

    /// Returns the region name as a string slice.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes `self` and returns the inner `Box<str>`.
    #[inline]
    #[must_use]
    pub fn into_boxed_str(self) -> Box<str> {
        self.0
    }
}

impl AsRef<str> for Region {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Region {
    type Err = InvalidRegion;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_regions() {
        let cases = [
            "us-east-1",
            "eu-west-1",
            "ap-southeast-2",
            "us-gov-west-1",
            "s3",
            "a",
            "123",
            "a-b-c-0",
        ];
        for s in cases {
            let r = Region::new(s.into());
            assert!(r.is_ok(), "expected valid: {s:?}");
            assert_eq!(r.unwrap().as_str(), s);
        }
    }

    #[test]
    fn invalid_regions() {
        let cases = [
            "",
            "US-EAST-1",
            "us_east_1",
            "us east 1",
            "us.east.1",
            "us-east-1!",
            "usEast1",
        ];
        for s in cases {
            let r = Region::new(s.into());
            assert!(r.is_err(), "expected invalid: {s:?}");
        }
    }

    #[test]
    fn from_str() {
        let r: Region = "us-west-2".parse().unwrap();
        assert_eq!(r.as_str(), "us-west-2");
    }

    #[test]
    fn display() {
        let r: Region = "eu-central-1".parse().unwrap();
        assert_eq!(format!("{r}"), "eu-central-1");
    }

    #[test]
    fn into_boxed_str() {
        let r: Region = "ap-south-1".parse().unwrap();
        let s = r.into_boxed_str();
        assert_eq!(&*s, "ap-south-1");
    }
}
