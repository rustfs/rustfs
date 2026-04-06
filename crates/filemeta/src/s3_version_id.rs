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

/// Wasabi `URLFriendlyChars` for the random suffix of `createVersionId` (10 chars).
/// Byte-for-byte parity with Wasabi: duplicate `e`/`z`, no `x` in the letter run.
pub const WASABI_VERSION_ID_RANDOM_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcedefghijklmnopqrstuvwzyz0123456789-_";

/// O(1) membership for the 10-character suffix of strict Wasabi `createVersionId` strings.
const WASABI_RANDOM_SUFFIX_CHAR_OK: [bool; 256] = {
    let mut lut = [false; 256];
    let bytes = WASABI_VERSION_ID_RANDOM_ALPHABET;
    let mut i = 0;
    while i < bytes.len() {
        lut[bytes[i] as usize] = true;
        i += 1;
    }
    lut
};

/// Validates strict Wasabi `createVersionId` shape and copies into a stack array in one pass.
///
/// Returns [`None`] if `s` is not exactly 32 bytes or does not match
/// `[0-9]{21}-` + 10 characters from [`WASABI_VERSION_ID_RANDOM_ALPHABET`].
#[inline]
fn try_copy_strict_wasabi_version_id(s: &str) -> Option<[u8; 32]> {
    let b = s.as_bytes();
    if b.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..21 {
        let c = b[i];
        if !c.is_ascii_digit() {
            return None;
        }
        out[i] = c;
    }
    if b[21] != b'-' {
        return None;
    }
    out[21] = b'-';
    for i in 22..32 {
        let c = b[i];
        if !WASABI_RANDOM_SUFFIX_CHAR_OK[c as usize] {
            return None;
        }
        out[i] = c;
    }
    Some(out)
}

/// `true` if `s` is exactly 32 bytes and matches Wasabi `createVersionId` shape:
/// `[0-9]{21}-` + 10 characters from [`WASABI_VERSION_ID_RANDOM_ALPHABET`].
#[inline]
pub fn is_strict_wasabi_create_version_id(s: &str) -> bool {
    try_copy_strict_wasabi_version_id(s).is_some()
}
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
    ///
    /// On failure, error messages are phrased for operators (xl.meta / object metadata decode).
    /// Wasabi-shaped ids are stored as UTF-8 ASCII; non-UTF-8 32-byte blobs are rejected as corrupt metadata.
    pub fn from_msgpack_id_bytes(buf: &[u8]) -> Result<Option<Self>> {
        match buf.len() {
            16 => {
                let arr: [u8; 16] = buf
                    .try_into()
                    .map_err(|_| Error::other("invalid version id in object metadata: expected 16 bytes for UUID form"))?;
                let u = Uuid::from_bytes(arr);
                if u.is_nil() { Ok(None) } else { Ok(Some(Self::Uuid(u))) }
            }
            32 => {
                let arr: [u8; 32] = buf.try_into().map_err(|_| {
                    Error::other("invalid version id in object metadata: expected 32 bytes for Wasabi ASCII form")
                })?;
                if std::str::from_utf8(arr.as_slice()).is_err() {
                    return Err(Error::other(
                        "invalid version id in object metadata: 32-byte Wasabi form must be valid UTF-8",
                    ));
                }
                Ok(Some(Self::WasabiAscii(arr)))
            }
            n => Err(Error::other(format!(
                "invalid version id in object metadata: expected 16 or 32 byte binary ID field, got length {n}"
            ))),
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

    /// Strict parse for inbound `X-Wasabi-Set-Version-Id` (Phase 2: shape + charset, length 32).
    pub fn parse_x_wasabi_set_version_id(s: &str) -> Result<Self> {
        let t = s.trim();
        if t.len() != 32 {
            return Err(Error::other(format!("X-Wasabi-Set-Version-Id must be 32 characters, got {}", t.len())));
        }
        let Some(a) = try_copy_strict_wasabi_version_id(t) else {
            return Err(Error::other(
                "X-Wasabi-Set-Version-Id: value does not match required version id format".to_owned(),
            ));
        };
        Ok(Self::WasabiAscii(a))
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
                if std::str::from_utf8(arr.as_slice()).is_err() {
                    return Err(E::custom("invalid version id: 32-byte Wasabi form must be valid UTF-8"));
                }
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

    #[test]
    fn parse_x_wasabi_set_version_id_strict() {
        let ok = "000000000000000000001-ABCDEabcd0";
        assert_eq!(ok.len(), 32);
        let id = S3VersionId::parse_x_wasabi_set_version_id(ok).unwrap();
        assert_eq!(id.to_string(), ok);
        assert!(S3VersionId::parse_x_wasabi_set_version_id("short").is_err());
        // Wrong charset in suffix (x not in alphabet)
        assert!(S3VersionId::parse_x_wasabi_set_version_id("000000000000000000001-xxxxxxxxxx").is_err());
    }

    #[test]
    fn parse_x_wasabi_set_version_id_table_rejects_bad_shapes() {
        let bad = [
            "00000000000000000000-ABCDEabcd01",  // only 20 digits before '-'
            "00000000000000000000A-ABCDEabcd0",  // non-digit in prefix region
            "000000000000000000001XABCDEabcd0",  // missing '-' at position 21
            "000000000000000000001-ABCDEabcd",   // 31 chars
            "000000000000000000001-ABCDEabcd01", // 33 chars
        ];
        for s in bad {
            assert!(S3VersionId::parse_x_wasabi_set_version_id(s).is_err(), "expected reject for {s:?}");
        }
    }

    #[test]
    fn parse_api_version_id_len_boundaries() {
        assert!(S3VersionId::parse_api_version_id("0123456789012345678901234567890").is_err()); // 31
        assert!(S3VersionId::parse_api_version_id("012345678901234567890123456789012").is_err()); // 33
        let u = Uuid::new_v4();
        assert!(S3VersionId::parse_api_version_id(&u.to_string()).is_ok());
    }

    #[test]
    fn from_msgpack_id_bytes_rejects_bad_length() {
        let err = S3VersionId::from_msgpack_id_bytes(&[0u8; 8]).unwrap_err();
        assert!(err.to_string().contains("expected 16 or 32 byte binary ID field"), "{err}");
    }

    #[test]
    fn from_msgpack_id_bytes_rejects_non_utf8_wasabi_length() {
        let mut b = [0xFFu8; 32];
        b[0] = b'0';
        b[1] = b'1';
        let err = S3VersionId::from_msgpack_id_bytes(b.as_slice()).unwrap_err();
        assert!(err.to_string().contains("must be valid UTF-8"), "{err}");
    }
}
