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

//! S3 `VersionId` on disk and on the wire: legacy UUID (16-byte `bin`) or Wasabi-style 32-byte ASCII.
//!
//! This type is [`Copy`] and uses only stack storage—no heap in the common case. Use [`S3VersionId::wire_slice`]
//! for hashing and msgpack; use [`fmt::Display`] only when formatting API strings (allocates a formatter write).

use crate::{Error, NULL_VERSION_ID, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::cmp::Ordering;
use std::fmt;
use uuid::Uuid;

/// Canonical version identity for one object version (xl/filemeta + API).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum S3VersionId {
    /// Legacy RustFS / S3 UUID form (16 bytes on wire).
    Uuid(Uuid),
    /// Wasabi `createVersionId`-shaped id (exactly 32 ASCII bytes on wire).
    WasabiAscii([u8; 32]),
}

impl Default for S3VersionId {
    fn default() -> Self {
        Self::Uuid(Uuid::nil())
    }
}

impl PartialOrd for S3VersionId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for S3VersionId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Uuid(a), Self::Uuid(b)) => a.as_bytes().cmp(b.as_bytes()),
            (Self::WasabiAscii(a), Self::WasabiAscii(b)) => a.cmp(b),
            (Self::Uuid(_), Self::WasabiAscii(_)) => Ordering::Less,
            (Self::WasabiAscii(_), Self::Uuid(_)) => Ordering::Greater,
        }
    }
}

impl fmt::Display for S3VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Uuid(u) => write!(f, "{u}"),
            Self::WasabiAscii(a) => {
                // Wasabi ids are ASCII; avoid lossy on hot Display if corrupt on disk.
                if let Ok(s) = std::str::from_utf8(a.as_slice()) {
                    f.write_str(s)
                } else {
                    write!(f, "invalid-wasabi-version-id")
                }
            }
        }
    }
}

impl S3VersionId {
    /// `true` for the nil UUID (versioning-off / placeholder); Wasabi ids are never nil.
    #[inline]
    pub fn is_nil(self) -> bool {
        matches!(self, Self::Uuid(u) if u.is_nil())
    }

    /// Borrowed wire bytes for msgpack / hashing (no allocation).
    #[inline]
    pub fn wire_slice(&self) -> &[u8] {
        match self {
            Self::Uuid(u) => u.as_bytes(),
            Self::WasabiAscii(a) => a.as_slice(),
        }
    }

    /// Decode msgpack `bin` for `ID` / header `version_id` (16-byte UUID or 32-byte Wasabi).
    pub fn from_msgpack_id_bytes(buf: &[u8]) -> Result<Option<Self>> {
        match buf.len() {
            16 => {
                let arr: [u8; 16] = buf.try_into().map_err(|_| Error::other("version id: expected 16 bytes"))?;
                let u = Uuid::from_bytes(arr);
                if u.is_nil() { Ok(None) } else { Ok(Some(Self::Uuid(u))) }
            }
            32 => {
                let arr: [u8; 32] = buf.try_into().map_err(|_| Error::other("version id: expected 32 bytes"))?;
                Ok(Some(Self::WasabiAscii(arr)))
            }
            n => Err(Error::other(format!("invalid version id binary length: expected 16 or 32, got {n}"))),
        }
    }

    /// Parse S3 `versionId` query/body (`null`, UUID, or 32-char ASCII Wasabi-shaped string).
    ///
    /// Exactly 32 ASCII bytes are treated as Wasabi-style ids **before** [`Uuid::parse_str`], because the
    /// UUID parser accepts some 32-character hex forms that would otherwise collide with that shape.
    pub fn parse_api_version_id(s: &str) -> Result<Option<Self>> {
        if s.is_empty() || s == NULL_VERSION_ID {
            return Ok(None);
        }
        if s.len() == 32 && s.is_ascii() {
            let mut a = [0u8; 32];
            a.copy_from_slice(s.as_bytes());
            return Ok(Some(Self::WasabiAscii(a)));
        }
        if let Ok(u) = Uuid::parse_str(s) {
            return Ok(Some(Self::Uuid(u)));
        }
        Err(Error::UuidParse(format!("invalid version id: {s}")))
    }
}

impl Serialize for S3VersionId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(self.wire_slice())
    }
}

struct S3VersionIdSerdeVisitor;

impl Visitor<'_> for S3VersionIdSerdeVisitor {
    type Value = S3VersionId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a 16- or 32-byte version id")
    }

    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> std::result::Result<Self::Value, E> {
        match v.len() {
            16 => {
                let arr: [u8; 16] = v.try_into().map_err(|_| E::custom("version id: expected 16 bytes"))?;
                Ok(S3VersionId::Uuid(Uuid::from_bytes(arr)))
            }
            32 => {
                let arr: [u8; 32] = v.try_into().map_err(|_| E::custom("version id: expected 32 bytes"))?;
                Ok(S3VersionId::WasabiAscii(arr))
            }
            n => Err(E::custom(format!("version id: expected 16 or 32 bytes, got {n}"))),
        }
    }
}

impl<'de> Deserialize<'de> for S3VersionId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(S3VersionIdSerdeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_uuid_msgpack_bytes() {
        let u = Uuid::new_v4();
        let id = S3VersionId::from_msgpack_id_bytes(u.as_bytes()).unwrap().unwrap();
        assert_eq!(id, S3VersionId::Uuid(u));
        assert_eq!(id.wire_slice(), u.as_bytes());
    }

    #[test]
    fn nil_uuid_decodes_to_none() {
        assert!(S3VersionId::from_msgpack_id_bytes(Uuid::nil().as_bytes()).unwrap().is_none());
    }

    #[test]
    fn round_trip_wasabi_msgpack_bytes() {
        let s = b"01234567890123456789012345678901";
        let id = S3VersionId::from_msgpack_id_bytes(s.as_slice()).unwrap().unwrap();
        assert_eq!(id, S3VersionId::WasabiAscii(*s));
        assert_eq!(id.to_string(), std::str::from_utf8(s).unwrap());
    }

    #[test]
    fn parse_api_accepts_uuid_and_wasabi_len32() {
        let u = Uuid::new_v4();
        assert_eq!(S3VersionId::parse_api_version_id(&u.to_string()).unwrap(), Some(S3VersionId::Uuid(u)));
        let w = "01234567890123456789012345678901";
        let mut wasabi = [0u8; 32];
        wasabi.copy_from_slice(w.as_bytes());
        assert_eq!(S3VersionId::parse_api_version_id(w).unwrap(), Some(S3VersionId::WasabiAscii(wasabi)));
        assert!(S3VersionId::parse_api_version_id("").unwrap().is_none());
        assert!(S3VersionId::parse_api_version_id(NULL_VERSION_ID).unwrap().is_none());
    }
}
