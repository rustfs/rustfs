// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The region name a RustFS deployment answers for.
//!
//! Responsible for: the validated region string the engine crates carry
//! (rustfs/backlog#2745), with the grammar the legacy S3 edge enforces: a
//! non-empty string of `[a-z0-9-]`. Not responsible for: resolving the region
//! from configuration or rendering it on the wire; the edge converts this type
//! to and from its own region type in `compat_s3s`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;

/// A validated region name: non-empty, only `[a-z0-9-]`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Region(Box<str>);

/// The string refused by [`Region::new`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidRegion(Box<str>);

impl fmt::Display for InvalidRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid region name: {:?}", self.0)
    }
}

impl std::error::Error for InvalidRegion {}

impl Region {
    fn is_valid(s: &str) -> bool {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }

    /// Validates `s` as a region name.
    pub fn new(s: impl Into<Box<str>>) -> Result<Self, InvalidRegion> {
        let s = s.into();
        if Self::is_valid(&s) {
            Ok(Self(s))
        } else {
            Err(InvalidRegion(s))
        }
    }

    /// The region name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The region name as its owned string.
    #[must_use]
    pub fn into_boxed_str(self) -> Box<str> {
        self.0
    }
}

impl AsRef<str> for Region {
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
        Self::new(s)
    }
}

impl Serialize for Region {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Region {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_lowercase_digits_and_dashes_only() {
        assert_eq!(Region::new("us-east-1").unwrap().as_str(), "us-east-1");
        assert_eq!("eu-west-2".parse::<Region>().unwrap().to_string(), "eu-west-2");
        for bad in ["", "US-EAST-1", "us east", "us_east", "ü"] {
            assert!(Region::new(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn serde_round_trips_as_a_plain_string() {
        let region = Region::new("ap-south-1").unwrap();
        let json = serde_json::to_string(&region).unwrap();
        assert_eq!(json, "\"ap-south-1\"");
        assert_eq!(serde_json::from_str::<Region>(&json).unwrap(), region);
        assert!(serde_json::from_str::<Region>("\"Bad Region\"").is_err());
    }
}
