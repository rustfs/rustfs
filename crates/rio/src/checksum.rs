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

use crate::errors::ChecksumMismatch;
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use http::HeaderMap;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::io::Write;

pub const SHA256_SIZE: usize = 32;

/// RustFS multipart checksum metadata key
pub const RUSTFS_MULTIPART_CHECKSUM: &str = "x-rustfs-multipart-checksum";

/// RustFS multipart checksum type metadata key  
pub const RUSTFS_MULTIPART_CHECKSUM_TYPE: &str = "x-rustfs-multipart-checksum-type";

/// Checksum type enumeration with flags
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ChecksumType(pub u32);

impl ChecksumType {
    /// Checksum will be sent in trailing header
    pub const TRAILING: ChecksumType = ChecksumType(1 << 0);

    /// SHA256 checksum
    pub const SHA256: ChecksumType = ChecksumType(1 << 1);

    /// SHA1 checksum
    pub const SHA1: ChecksumType = ChecksumType(1 << 2);

    /// CRC32 checksum with IEEE table
    pub const CRC32: ChecksumType = ChecksumType(1 << 3);

    /// CRC32 checksum with Castagnoli table
    pub const CRC32C: ChecksumType = ChecksumType(1 << 4);

    /// Invalid checksum
    pub const INVALID: ChecksumType = ChecksumType(1 << 5);

    /// Multipart checksum
    pub const MULTIPART: ChecksumType = ChecksumType(1 << 6);

    /// Checksum includes multipart checksums
    pub const INCLUDES_MULTIPART: ChecksumType = ChecksumType(1 << 7);

    /// CRC64 with NVME polynomial
    pub const CRC64_NVME: ChecksumType = ChecksumType(1 << 8);

    /// Full object checksum
    pub const FULL_OBJECT: ChecksumType = ChecksumType(1 << 9);

    // --- S3 additional checksum algorithms (AWS 2026-04). New base-type bits are
    // append-only above bit 9; existing bits must never be renumbered because the
    // raw `checksum_type.0` value is varint-serialized into xl.meta (see append_to).

    /// XXHash3 (64-bit) checksum. COMPOSITE-only per AWS.
    pub const XXHASH3: ChecksumType = ChecksumType(1 << 10);

    /// XXHash64 checksum.
    pub const XXHASH64: ChecksumType = ChecksumType(1 << 11);

    /// XXHash128 checksum.
    pub const XXHASH128: ChecksumType = ChecksumType(1 << 12);

    /// SHA-512 checksum.
    pub const SHA512: ChecksumType = ChecksumType(1 << 13);

    /// MD5 as an ADDITIONAL checksum (x-amz-checksum-md5), distinct from the legacy
    /// Content-MD5 / ETag path.
    pub const MD5: ChecksumType = ChecksumType(1 << 14);

    /// No checksum
    pub const NONE: ChecksumType = ChecksumType(0);

    /// Base-type mask, derived from [`BASE_CHECKSUM_TYPES`] as the single source of
    /// truth. A new base type added to that list is automatically covered here, so
    /// `base()` can never silently strip a wired-up algorithm (see #1252 / #1254).
    const BASE_TYPE_MASK: u32 = compute_base_type_mask(BASE_CHECKSUM_TYPES);

    /// Check if this checksum type has all flags of the given type
    pub fn is(self, t: ChecksumType) -> bool {
        if t == Self::NONE {
            return self == Self::NONE;
        }
        (self.0 & t.0) == t.0
    }

    /// Merge another checksum type into this one
    pub fn merge(&mut self, other: ChecksumType) -> &mut Self {
        self.0 |= other.0;
        self
    }

    /// Get the base checksum type (without flags)
    pub fn base(self) -> ChecksumType {
        ChecksumType(self.0 & Self::BASE_TYPE_MASK)
    }

    /// Get the header key for this checksum type
    pub fn key(self) -> Option<&'static str> {
        match self.base() {
            Self::CRC32 => Some("x-amz-checksum-crc32"),
            Self::CRC32C => Some("x-amz-checksum-crc32c"),
            Self::SHA1 => Some("x-amz-checksum-sha1"),
            Self::SHA256 => Some("x-amz-checksum-sha256"),
            Self::CRC64_NVME => Some("x-amz-checksum-crc64nvme"),
            Self::XXHASH3 => Some("x-amz-checksum-xxhash3"),
            Self::XXHASH64 => Some("x-amz-checksum-xxhash64"),
            Self::XXHASH128 => Some("x-amz-checksum-xxhash128"),
            Self::SHA512 => Some("x-amz-checksum-sha512"),
            Self::MD5 => Some("x-amz-checksum-md5"),
            _ => None,
        }
    }

    /// Get the size of the raw (unencoded) checksum in bytes
    pub fn raw_byte_len(self) -> usize {
        match self.base() {
            Self::CRC32 | Self::CRC32C => 4,
            Self::SHA1 => 20,
            Self::SHA256 => SHA256_SIZE,
            Self::CRC64_NVME => 8,
            Self::XXHASH3 | Self::XXHASH64 => 8,
            Self::XXHASH128 => 16,
            Self::SHA512 => 64,
            Self::MD5 => 16,
            _ => 0,
        }
    }

    /// Check if the checksum type is set and valid
    pub fn is_set(self) -> bool {
        !self.is(Self::INVALID) && !self.base().is(Self::NONE)
    }

    /// Check if this checksum type can be merged
    pub fn can_merge(self) -> bool {
        self.is(Self::CRC64_NVME) || self.is(Self::CRC32C) || self.is(Self::CRC32)
    }

    /// Create a hasher for this checksum type
    pub fn hasher(self) -> Option<Box<dyn ChecksumHasher>> {
        match self.base() {
            Self::CRC32 => Some(Box::new(Crc32IeeeHasher::new())),
            Self::CRC32C => Some(Box::new(Crc32CastagnoliHasher::new())),
            Self::SHA1 => Some(Box::new(Sha1Hasher::new())),
            Self::SHA256 => Some(Box::new(Sha256Hasher::new())),
            Self::CRC64_NVME => Some(Box::new(Crc64NvmeHasher::new())),
            Self::XXHASH3 => Some(Box::new(Xxh3Hasher::new())),
            Self::XXHASH64 => Some(Box::new(Xxh64Hasher::new())),
            Self::XXHASH128 => Some(Box::new(Xxh128Hasher::new())),
            Self::SHA512 => Some(Box::new(Sha512Hasher::new())),
            Self::MD5 => Some(Box::new(Md5Hasher::new())),
            _ => None,
        }
    }

    /// Check if checksum is trailing
    pub fn trailing(self) -> bool {
        self.is(Self::TRAILING)
    }

    /// Check if full object checksum was requested
    pub fn full_object_requested(self) -> bool {
        (self.0 & Self::FULL_OBJECT.0) == Self::FULL_OBJECT.0 || self.is(Self::CRC64_NVME)
    }

    /// True for the five algorithms s3s exposes as typed `*Output` fields
    /// (CRC32/CRC32C/SHA1/SHA256/CRC64NVME). The AWS 2026-04 additional algorithms
    /// (XXHash3/64/128, SHA-512, MD5) have no typed field and are carried as raw
    /// response headers instead. Single source of truth for that split.
    pub fn is_s3s_typed(self) -> bool {
        matches!(self.base(), Self::CRC32 | Self::CRC32C | Self::SHA1 | Self::SHA256 | Self::CRC64_NVME)
    }

    /// Get object type string for x-amz-checksum-type header
    pub fn obj_type(self) -> &'static str {
        if self.full_object_requested() {
            "FULL_OBJECT"
        } else if self.is_set() {
            "COMPOSITE"
        } else {
            ""
        }
    }

    pub fn from_header(headers: &HeaderMap) -> Self {
        Self::from_string_with_obj_type(
            headers
                .get("x-amz-checksum-algorithm")
                .and_then(|v| v.to_str().ok())
                .unwrap_or(""),
            headers.get("x-amz-checksum-type").and_then(|v| v.to_str().ok()).unwrap_or(""),
        )
    }

    /// Create checksum type from string algorithm
    pub fn from_string(alg: &str) -> Self {
        Self::from_string_with_obj_type(alg, "")
    }

    /// Create checksum type from algorithm and object type
    pub fn from_string_with_obj_type(alg: &str, obj_type: &str) -> Self {
        let full = match obj_type {
            "FULL_OBJECT" => Self::FULL_OBJECT,
            "COMPOSITE" | "" => Self::NONE,
            _ => return Self::INVALID,
        };

        // Case-insensitive matching WITHOUT allocating: to_uppercase() allocated a
        // String on every checksummed request, and this path is hot. Composite-only
        // algorithms reject an explicit FULL_OBJECT request — they cannot be linearly
        // combined into a full-object checksum the way CRCs can (same rule as SHA1/256).
        let composite_only = |ty: ChecksumType| -> ChecksumType { if full != Self::NONE { Self::INVALID } else { ty } };

        if alg.eq_ignore_ascii_case("CRC32") {
            ChecksumType(Self::CRC32.0 | full.0)
        } else if alg.eq_ignore_ascii_case("CRC32C") {
            ChecksumType(Self::CRC32C.0 | full.0)
        } else if alg.eq_ignore_ascii_case("CRC64NVME") {
            // AWS ignores the full-object flag here and just assumes it.
            Self::CRC64_NVME
        } else if alg.eq_ignore_ascii_case("SHA1") {
            composite_only(Self::SHA1)
        } else if alg.eq_ignore_ascii_case("SHA256") {
            composite_only(Self::SHA256)
        } else if alg.eq_ignore_ascii_case("SHA512") {
            composite_only(Self::SHA512)
        } else if alg.eq_ignore_ascii_case("XXHASH3") {
            composite_only(Self::XXHASH3)
        } else if alg.eq_ignore_ascii_case("XXHASH64") {
            composite_only(Self::XXHASH64)
        } else if alg.eq_ignore_ascii_case("XXHASH128") {
            composite_only(Self::XXHASH128)
        } else if alg.eq_ignore_ascii_case("MD5") {
            composite_only(Self::MD5)
        } else if alg.is_empty() {
            composite_only(Self::NONE)
        } else {
            Self::INVALID
        }
    }
}

impl std::fmt::Display for ChecksumType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.base() {
            Self::CRC32 => write!(f, "CRC32"),
            Self::CRC32C => write!(f, "CRC32C"),
            Self::SHA1 => write!(f, "SHA1"),
            Self::SHA256 => write!(f, "SHA256"),
            Self::CRC64_NVME => write!(f, "CRC64NVME"),
            Self::XXHASH3 => write!(f, "XXHASH3"),
            Self::XXHASH64 => write!(f, "XXHASH64"),
            Self::XXHASH128 => write!(f, "XXHASH128"),
            Self::SHA512 => write!(f, "SHA512"),
            Self::MD5 => write!(f, "MD5"),
            Self::NONE => write!(f, ""),
            _ => write!(f, "invalid"),
        }
    }
}

/// Base checksum types list. This is the single source of truth for which base
/// algorithms exist: [`ChecksumType::BASE_TYPE_MASK`] is derived from it, so adding
/// a new algorithm here cannot leave `base()` silently stripping it (see #1254).
pub const BASE_CHECKSUM_TYPES: &[ChecksumType] = &[
    ChecksumType::SHA256,
    ChecksumType::SHA1,
    ChecksumType::CRC32,
    ChecksumType::CRC64_NVME,
    ChecksumType::CRC32C,
    ChecksumType::XXHASH3,
    ChecksumType::XXHASH64,
    ChecksumType::XXHASH128,
    ChecksumType::SHA512,
    ChecksumType::MD5,
];

/// Fold the base-type bits of `types` into a mask. Used to derive
/// [`ChecksumType::BASE_TYPE_MASK`] from [`BASE_CHECKSUM_TYPES`] at compile time.
const fn compute_base_type_mask(types: &[ChecksumType]) -> u32 {
    let mut mask = 0u32;
    let mut i = 0;
    while i < types.len() {
        mask |= types[i].0;
        i += 1;
    }
    mask
}

/// Checksum structure containing type and encoded value
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Checksum {
    pub checksum_type: ChecksumType,
    pub encoded: String,
    pub raw: Vec<u8>,
    pub want_parts: i32,
}

impl Checksum {
    /// Create a new checksum from data
    pub fn new_from_data(checksum_type: ChecksumType, data: &[u8]) -> Option<Self> {
        if !checksum_type.is_set() {
            return None;
        }

        let mut hasher = checksum_type.hasher()?;
        hasher.write_all(data).ok()?;
        let raw = hasher.finalize();
        let encoded = general_purpose::STANDARD.encode(&raw);

        let checksum = Checksum {
            checksum_type,
            encoded,
            raw,
            want_parts: 0,
        };

        if checksum.valid() { Some(checksum) } else { None }
    }

    /// Create a new checksum from algorithm string and base64 value
    pub fn new_from_string(alg: &str, value: &str) -> Option<Self> {
        Self::new_with_type(ChecksumType::from_string(alg), value)
    }

    /// Create a new checksum with specific type and value
    pub fn new_with_type(mut checksum_type: ChecksumType, value: &str) -> Option<Self> {
        if !checksum_type.is_set() {
            return None;
        }

        let mut want_parts = 0;
        let value_string;

        // Handle multipart format (value-parts)
        if value.contains('-') {
            let parts: Vec<&str> = value.split('-').collect();
            if parts.len() != 2 {
                return None;
            }
            value_string = parts[0].to_string();
            want_parts = parts[1].parse().ok()?;
            checksum_type = ChecksumType(checksum_type.0 | ChecksumType::MULTIPART.0);
        } else {
            value_string = value.to_string();
        }
        // let raw = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(&value_string).ok()?;
        let raw = general_purpose::STANDARD.decode(&value_string).ok()?;

        let checksum = Checksum {
            checksum_type,
            encoded: value_string,
            raw,
            want_parts,
        };

        if checksum.valid() { Some(checksum) } else { None }
    }

    /// Check if checksum is valid
    pub fn valid(&self) -> bool {
        if self.checksum_type == ChecksumType::INVALID {
            return false;
        }
        if self.encoded.is_empty() || self.checksum_type.trailing() {
            return self.checksum_type.is(ChecksumType::NONE) || self.checksum_type.trailing();
        }
        self.checksum_type.raw_byte_len() == self.raw.len()
    }

    /// Check if content matches this checksum
    pub fn matches(&self, content: &[u8], parts: i32) -> Result<(), ChecksumMismatch> {
        if self.encoded.is_empty() {
            return Ok(());
        }

        let mut hasher = self.checksum_type.hasher().ok_or_else(|| ChecksumMismatch {
            want: self.encoded.clone(),
            got: "no hasher available".to_string(),
        })?;

        hasher.write_all(content).map_err(|_| ChecksumMismatch {
            want: self.encoded.clone(),
            got: "write error".to_string(),
        })?;

        let sum = hasher.finalize();

        if self.want_parts > 0 && self.want_parts != parts {
            return Err(ChecksumMismatch {
                want: format!("{}-{}", self.encoded, self.want_parts),
                got: format!("{}-{}", general_purpose::STANDARD.encode(&sum), parts),
            });
        }

        if sum != self.raw {
            return Err(ChecksumMismatch {
                want: self.encoded.clone(),
                got: general_purpose::STANDARD.encode(&sum),
            });
        }

        Ok(())
    }

    /// Convert checksum to map representation
    pub fn as_map(&self) -> Option<HashMap<String, String>> {
        if !self.valid() {
            return None;
        }
        let mut map = HashMap::new();
        map.insert(self.checksum_type.to_string(), self.encoded.clone());
        Some(map)
    }

    pub fn to_bytes(&self, parts: &[u8]) -> Bytes {
        self.append_to(Vec::new(), parts).into()
    }

    /// Append checksum to byte buffer
    pub fn append_to(&self, mut buffer: Vec<u8>, parts: &[u8]) -> Vec<u8> {
        // Encode checksum type as varint
        let mut type_bytes = Vec::new();
        encode_varint(&mut type_bytes, self.checksum_type.0 as u64);
        buffer.extend_from_slice(&type_bytes);

        // Remove trailing flag when serializing
        let crc = self.raw.clone();
        if self.checksum_type.trailing() {
            // When serializing, we don't care if it was trailing
        }

        if crc.len() != self.checksum_type.raw_byte_len() {
            return buffer;
        }

        buffer.extend_from_slice(&crc);

        if self.checksum_type.is(ChecksumType::MULTIPART) {
            let mut checksums = 0;
            if self.want_parts > 0 && !self.checksum_type.is(ChecksumType::INCLUDES_MULTIPART) {
                checksums = self.want_parts;
            }

            // Ensure we don't divide by 0
            let raw_len = self.checksum_type.raw_byte_len();
            if raw_len == 0 || !parts.len().is_multiple_of(raw_len) {
                checksums = 0;
            } else if !parts.is_empty() {
                checksums = (parts.len() / raw_len) as i32;
            }

            let parts_to_append = if self.checksum_type.is(ChecksumType::INCLUDES_MULTIPART) {
                parts
            } else {
                &[]
            };

            let mut checksums_bytes = Vec::new();
            encode_varint(&mut checksums_bytes, checksums as u64);
            buffer.extend_from_slice(&checksums_bytes);

            if !parts_to_append.is_empty() {
                buffer.extend_from_slice(parts_to_append);
            }
        }

        buffer
    }

    /// Add a part checksum into the current checksum, as if the content of each was appended.
    /// The size of the content that produced the second checksum must be provided.
    /// Not all checksum types can be merged, use the can_merge method to check.
    /// Checksum types must match.
    pub fn add_part(&mut self, other: &Checksum, size: i64) -> Result<(), String> {
        if !other.checksum_type.can_merge() {
            return Err("checksum type cannot be merged".to_string());
        }

        if size == 0 {
            return Ok(());
        }

        if !self.checksum_type.is(other.checksum_type.base()) {
            return Err(format!(
                "checksum type does not match got {} and {}",
                self.checksum_type, other.checksum_type
            ));
        }

        // If never set, just add first checksum
        if self.raw.is_empty() {
            self.raw = other.raw.clone();
            self.encoded = other.encoded.clone();
            return Ok(());
        }

        if !self.valid() {
            return Err("invalid base checksum".to_string());
        }

        if !other.valid() {
            return Err("invalid part checksum".to_string());
        }

        match self.checksum_type.base() {
            ChecksumType::CRC32 => {
                let crc1 = u32::from_be_bytes([self.raw[0], self.raw[1], self.raw[2], self.raw[3]]);
                let crc2 = u32::from_be_bytes([other.raw[0], other.raw[1], other.raw[2], other.raw[3]]);
                let combined = crc32_combine(0xEDB88320, crc1, crc2, size); // IEEE polynomial
                self.raw = combined.to_be_bytes().to_vec();
            }
            ChecksumType::CRC32C => {
                let crc1 = u32::from_be_bytes([self.raw[0], self.raw[1], self.raw[2], self.raw[3]]);
                let crc2 = u32::from_be_bytes([other.raw[0], other.raw[1], other.raw[2], other.raw[3]]);
                let combined = crc32_combine(0x82F63B78, crc1, crc2, size); // Castagnoli polynomial
                self.raw = combined.to_be_bytes().to_vec();
            }
            ChecksumType::CRC64_NVME => {
                let crc1 = u64::from_be_bytes([
                    self.raw[0],
                    self.raw[1],
                    self.raw[2],
                    self.raw[3],
                    self.raw[4],
                    self.raw[5],
                    self.raw[6],
                    self.raw[7],
                ]);
                let crc2 = u64::from_be_bytes([
                    other.raw[0],
                    other.raw[1],
                    other.raw[2],
                    other.raw[3],
                    other.raw[4],
                    other.raw[5],
                    other.raw[6],
                    other.raw[7],
                ]);
                let combined = crc64_combine(CRC64_NVME_POLYNOMIAL.reverse_bits(), crc1, crc2, size);
                self.raw = combined.to_be_bytes().to_vec();
            }
            _ => {
                return Err(format!("unknown checksum type: {}", self.checksum_type));
            }
        }

        self.encoded = general_purpose::STANDARD.encode(&self.raw);
        Ok(())
    }
}

/// Get content checksum from headers
pub fn get_content_checksum(headers: &HeaderMap) -> Result<Option<Checksum>, std::io::Error> {
    // Check for trailing checksums
    if let Some(trailer_header) = headers.get("x-amz-trailer") {
        let mut result = None;
        let trailer_str = trailer_header
            .to_str()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid header value"))?;
        let trailing_headers: Vec<&str> = trailer_str.split(',').map(|s| s.trim()).collect();

        for header in trailing_headers {
            let mut duplicates = false;
            for &checksum_type in BASE_CHECKSUM_TYPES {
                if let Some(key) = checksum_type.key()
                    && header.eq_ignore_ascii_case(key)
                {
                    duplicates = result.is_some();
                    result = Some(Checksum {
                        checksum_type: ChecksumType(checksum_type.0 | ChecksumType::TRAILING.0),
                        encoded: String::new(),
                        raw: Vec::new(),
                        want_parts: 0,
                    });
                }
            }
            if duplicates {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid checksum"));
            }
        }

        if let Some(mut res) = result {
            match headers.get("x-amz-checksum-type").and_then(|v| v.to_str().ok()) {
                Some("FULL_OBJECT") => {
                    if !res.checksum_type.can_merge() {
                        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid checksum"));
                    }
                    res.checksum_type = ChecksumType(res.checksum_type.0 | ChecksumType::FULL_OBJECT.0);
                }
                Some("COMPOSITE") | Some("") | None => {}
                _ => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid checksum")),
            }
            return Ok(Some(res));
        }
    }

    let (checksum_type, value) = get_content_checksum_direct(headers);
    if checksum_type == ChecksumType::NONE {
        if value.is_empty() {
            return Ok(None);
        }
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid checksum"));
    }

    if checksum_type == ChecksumType::INVALID {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            crate::errors::ChecksumMismatch {
                want: "valid checksum header".to_string(),
                got: "invalid or duplicate checksum headers".to_string(),
            },
        ));
    }

    let checksum = Checksum::new_with_type(checksum_type, &value);
    if checksum.is_none() && !value.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            crate::errors::ChecksumMismatch {
                want: value,
                got: "invalid checksum value".to_string(),
            },
        ));
    }
    Ok(checksum)
}

/// Get content checksum type and value directly from headers
fn get_content_checksum_direct(headers: &HeaderMap) -> (ChecksumType, String) {
    let mut checksum_type = ChecksumType::NONE;

    if let Some(alg) = headers.get("x-amz-checksum-algorithm").and_then(|v| v.to_str().ok()) {
        checksum_type = ChecksumType::from_string_with_obj_type(
            alg,
            headers.get("x-amz-checksum-type").and_then(|s| s.to_str().ok()).unwrap_or(""),
        );

        if headers.get("x-amz-checksum-type").and_then(|v| v.to_str().ok()) == Some("FULL_OBJECT") {
            if !checksum_type.can_merge() {
                return (ChecksumType::INVALID, String::new());
            }
            checksum_type = ChecksumType(checksum_type.0 | ChecksumType::FULL_OBJECT.0);
        }

        if checksum_type.is_set()
            && let Some(key) = checksum_type.key()
        {
            return if let Some(value) = headers.get(key).and_then(|v| v.to_str().ok()) {
                (checksum_type, value.to_string())
            } else {
                (ChecksumType::NONE, String::new())
            };
        }
        return (checksum_type, String::new());
    }

    // Check individual checksum headers
    for &ct in BASE_CHECKSUM_TYPES {
        if let Some(key) = ct.key()
            && let Some(value) = headers.get(key).and_then(|v| v.to_str().ok())
        {
            // If already set, invalid
            if checksum_type != ChecksumType::NONE {
                return (ChecksumType::INVALID, String::new());
            }
            checksum_type = ct;

            if headers.get("x-amz-checksum-type").and_then(|v| v.to_str().ok()) == Some("FULL_OBJECT") {
                if !checksum_type.can_merge() {
                    return (ChecksumType::INVALID, String::new());
                }
                checksum_type = ChecksumType(checksum_type.0 | ChecksumType::FULL_OBJECT.0);
            }
            return (checksum_type, value.to_string());
        }
    }

    (checksum_type, String::new())
}

/// Trait for checksum hashers
pub trait ChecksumHasher: Write + Send + Sync {
    fn finalize(&mut self) -> Vec<u8>;
    fn reset(&mut self);
}

/// CRC32 IEEE hasher
pub struct Crc32IeeeHasher {
    hasher: crc_fast::Digest,
}

impl Default for Crc32IeeeHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32IeeeHasher {
    pub fn new() -> Self {
        Self {
            hasher: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc),
        }
    }
}

impl Write for Crc32IeeeHasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Crc32IeeeHasher {
    fn finalize(&mut self) -> Vec<u8> {
        (self.hasher.clone().finalize() as u32).to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
    }
}

/// CRC32 Castagnoli hasher
pub struct Crc32CastagnoliHasher {
    hasher: crc_fast::Digest,
}

impl Default for Crc32CastagnoliHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32CastagnoliHasher {
    pub fn new() -> Self {
        Self {
            hasher: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi),
        }
    }
}

impl Write for Crc32CastagnoliHasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Crc32CastagnoliHasher {
    fn finalize(&mut self) -> Vec<u8> {
        (self.hasher.clone().finalize() as u32).to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    }
}

/// SHA1 hasher
pub struct Sha1Hasher {
    hasher: Sha1,
}

impl Default for Sha1Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1Hasher {
    pub fn new() -> Self {
        Self { hasher: Sha1::new() }
    }
}

impl Write for Sha1Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Sha1Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.clone().finalize().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = Sha1::new();
    }
}

/// SHA256 hasher
pub struct Sha256Hasher {
    hasher: Sha256,
}

impl Default for Sha256Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256Hasher {
    pub fn new() -> Self {
        Self { hasher: Sha256::new() }
    }
}

impl Write for Sha256Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Sha256Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.clone().finalize().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = Sha256::new();
    }
}

/// CRC64 NVME hasher
pub struct Crc64NvmeHasher {
    hasher: crc_fast::Digest,
}

impl Default for Crc64NvmeHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc64NvmeHasher {
    pub fn new() -> Self {
        Self {
            hasher: crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme),
        }
    }
}

impl Write for Crc64NvmeHasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Crc64NvmeHasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.clone().finalize().to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc64Nvme);
    }
}

/// XXHash3 (64-bit) hasher, seed 0. Digest encoded big-endian (S3 canonical form,
/// matching the AWS CRT wire representation).
pub struct Xxh3Hasher {
    hasher: xxhash_rust::xxh3::Xxh3,
}

impl Default for Xxh3Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Xxh3Hasher {
    pub fn new() -> Self {
        Self {
            hasher: xxhash_rust::xxh3::Xxh3::new(),
        }
    }
}

impl Write for Xxh3Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Xxh3Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.digest().to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = xxhash_rust::xxh3::Xxh3::new();
    }
}

/// XXHash128 hasher, seed 0. Digest encoded big-endian over the full 128 bits.
pub struct Xxh128Hasher {
    hasher: xxhash_rust::xxh3::Xxh3,
}

impl Default for Xxh128Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Xxh128Hasher {
    pub fn new() -> Self {
        Self {
            hasher: xxhash_rust::xxh3::Xxh3::new(),
        }
    }
}

impl Write for Xxh128Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Xxh128Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.digest128().to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = xxhash_rust::xxh3::Xxh3::new();
    }
}

/// XXHash64 hasher, seed 0. Digest encoded big-endian.
pub struct Xxh64Hasher {
    hasher: xxhash_rust::xxh64::Xxh64,
}

impl Default for Xxh64Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Xxh64Hasher {
    pub fn new() -> Self {
        Self {
            hasher: xxhash_rust::xxh64::Xxh64::new(0),
        }
    }
}

impl Write for Xxh64Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Xxh64Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.digest().to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = xxhash_rust::xxh64::Xxh64::new(0);
    }
}

/// SHA-512 hasher.
pub struct Sha512Hasher {
    hasher: Sha512,
}

impl Default for Sha512Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha512Hasher {
    pub fn new() -> Self {
        Self { hasher: Sha512::new() }
    }
}

impl Write for Sha512Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Sha512Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.clone().finalize().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = Sha512::new();
    }
}

/// MD5 hasher for the ADDITIONAL checksum (x-amz-checksum-md5). Separate from the
/// legacy Content-MD5 / ETag machinery — this only serves the flexible-checksum path.
pub struct Md5Hasher {
    hasher: md5::Md5,
}

impl Default for Md5Hasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5Hasher {
    pub fn new() -> Self {
        use md5::Digest as _;
        Self { hasher: md5::Md5::new() }
    }
}

impl Write for Md5Hasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        use md5::Digest as _;
        self.hasher.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Md5Hasher {
    fn finalize(&mut self) -> Vec<u8> {
        use md5::Digest as _;
        self.hasher.clone().finalize().to_vec()
    }

    fn reset(&mut self) {
        use md5::Digest as _;
        self.hasher = md5::Md5::new();
    }
}

/// Encode unsigned integer as varint
fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Decode varint from buffer
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut pos = 0;

    for &byte in buf {
        pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some((result, pos));
        }

        shift += 7;
        if shift >= 64 {
            return None; // Overflow
        }
    }

    None // Incomplete varint
}

/// Read checksums from byte buffer
pub fn read_checksums(mut buf: &[u8], part: i32) -> (HashMap<String, String>, bool) {
    let mut result = HashMap::new();
    let mut is_multipart = false;

    while !buf.is_empty() {
        let (checksum_type_val, n) = match decode_varint(buf) {
            Some((val, n)) => (val, n),
            None => break,
        };
        buf = &buf[n..];

        let checksum_type = ChecksumType(checksum_type_val as u32);
        let length = checksum_type.raw_byte_len();

        if length == 0 || buf.len() < length {
            break;
        }

        let checksum_bytes = &buf[..length];
        buf = &buf[length..];
        let mut checksum_str = general_purpose::STANDARD.encode(checksum_bytes);

        if checksum_type.is(ChecksumType::MULTIPART) {
            is_multipart = true;
            let (parts_count, n) = match decode_varint(buf) {
                Some((val, n)) => (val, n),
                None => break,
            };
            buf = &buf[n..];

            if !checksum_type.full_object_requested() {
                checksum_str = format!("{checksum_str}-{parts_count}");
            } else if part <= 0 {
                result.insert("x-amz-checksum-type".to_string(), "FULL_OBJECT".to_string());
            }

            if part > 0 {
                checksum_str.clear();
            }

            if checksum_type.is(ChecksumType::INCLUDES_MULTIPART) {
                let want_len = parts_count as usize * length;
                if buf.len() < want_len {
                    break;
                }

                // Read part checksum
                if part > 0 && (part as u64) <= parts_count {
                    let offset = ((part - 1) as usize) * length;
                    let part_checksum = &buf[offset..offset + length];
                    checksum_str = general_purpose::STANDARD.encode(part_checksum);
                }
                buf = &buf[want_len..];
            }
        } else if part > 1 {
            // For non-multipart, checksum is part 1
            checksum_str.clear();
        }

        if !checksum_str.is_empty() {
            result.insert(checksum_type.to_string(), checksum_str);
        }
    }

    (result, is_multipart)
}

/// Read all part checksums from buffer
pub fn read_part_checksums(mut buf: &[u8]) -> Vec<HashMap<String, String>> {
    let mut result = Vec::new();

    while !buf.is_empty() {
        let (checksum_type_val, n) = match decode_varint(buf) {
            Some((val, n)) => (val, n),
            None => break,
        };
        buf = &buf[n..];

        let checksum_type = ChecksumType(checksum_type_val as u32);
        let length = checksum_type.raw_byte_len();

        if length == 0 || buf.len() < length {
            break;
        }

        // Skip main checksum
        buf = &buf[length..];

        let (parts_count, n) = match decode_varint(buf) {
            Some((val, n)) => (val, n),
            None => break,
        };
        buf = &buf[n..];

        if !checksum_type.is(ChecksumType::INCLUDES_MULTIPART) {
            continue;
        }

        if result.is_empty() {
            result.resize(parts_count as usize, HashMap::new());
        }

        for part_checksum in result.iter_mut() {
            if buf.len() < length {
                break;
            }

            let checksum_bytes = &buf[..length];
            buf = &buf[length..];
            let checksum_str = general_purpose::STANDARD.encode(checksum_bytes);

            part_checksum.insert(checksum_type.to_string(), checksum_str);
        }
    }

    result
}

/// CRC64 NVME polynomial constant
const CRC64_NVME_POLYNOMIAL: u64 = 0xad93d23594c93659;

/// GF(2) matrix multiplication
fn gf2_matrix_times(mat: &[u64], mut vec: u64) -> u64 {
    let mut sum = 0u64;
    for &m in mat {
        if vec == 0 {
            break;
        }

        if vec & 1 != 0 {
            sum ^= m;
        }
        vec >>= 1;
    }

    sum
}

/// Square a GF(2) matrix
fn gf2_matrix_square(square: &mut [u64], mat: &[u64]) {
    if square.len() != mat.len() {
        panic!("square matrix size mismatch");
    }

    for (i, &m) in mat.iter().enumerate() {
        square[i] = gf2_matrix_times(mat, m);
    }
}

/// Combine two CRC32 values
///
/// Returns the combined CRC-32 hash value of the two passed CRC-32
/// hash values crc1 and crc2. poly represents the generator polynomial
/// and len2 specifies the byte length that the crc2 hash covers.
fn crc32_combine(poly: u32, crc1: u32, crc2: u32, len2: i64) -> u32 {
    // Degenerate case (also disallow negative lengths)
    if len2 <= 0 {
        return crc1;
    }

    let mut even = [0u64; 32]; // even-power-of-two zeros operator
    let mut odd = [0u64; 32]; // odd-power-of-two zeros operator

    // Put operator for one zero bit in odd
    odd[0] = poly as u64; // CRC-32 polynomial
    let mut row = 1u64;
    for (_i, odd_val) in odd.iter_mut().enumerate().skip(1) {
        *odd_val = row;
        row <<= 1;
    }

    // Put operator for two zero bits in even
    gf2_matrix_square(&mut even, &odd);

    // Put operator for four zero bits in odd
    gf2_matrix_square(&mut odd, &even);

    // Apply len2 zeros to crc1 (first square will put the operator for one
    // zero byte, eight zero bits, in even)
    let mut crc1n = crc1 as u64;
    let mut len2 = len2;

    loop {
        // Apply zeros operator for this bit of len2
        gf2_matrix_square(&mut even, &odd);
        if len2 & 1 != 0 {
            crc1n = gf2_matrix_times(&even, crc1n);
        }
        len2 >>= 1;

        // If no more bits set, then done
        if len2 == 0 {
            break;
        }

        // Another iteration of the loop with odd and even swapped
        gf2_matrix_square(&mut odd, &even);
        if len2 & 1 != 0 {
            crc1n = gf2_matrix_times(&odd, crc1n);
        }
        len2 >>= 1;

        // If no more bits set, then done
        if len2 == 0 {
            break;
        }
    }

    // Return combined crc
    crc1n ^= crc2 as u64;
    crc1n as u32
}

/// Combine two CRC64 values
fn crc64_combine(poly: u64, crc1: u64, crc2: u64, len2: i64) -> u64 {
    // Degenerate case (also disallow negative lengths)
    if len2 <= 0 {
        return crc1;
    }

    let mut even = [0u64; 64]; // even-power-of-two zeros operator
    let mut odd = [0u64; 64]; // odd-power-of-two zeros operator

    // Put operator for one zero bit in odd
    odd[0] = poly; // CRC-64 polynomial
    let mut row = 1u64;
    for (_i, odd_val) in odd.iter_mut().enumerate().skip(1) {
        *odd_val = row;
        row <<= 1;
    }

    // Put operator for two zero bits in even
    gf2_matrix_square(&mut even, &odd);

    // Put operator for four zero bits in odd
    gf2_matrix_square(&mut odd, &even);

    // Apply len2 zeros to crc1 (first square will put the operator for one
    // zero byte, eight zero bits, in even)
    let mut crc1n = crc1;
    let mut len2 = len2;

    loop {
        // Apply zeros operator for this bit of len2
        gf2_matrix_square(&mut even, &odd);
        if len2 & 1 != 0 {
            crc1n = gf2_matrix_times(&even, crc1n);
        }
        len2 >>= 1;

        // If no more bits set, then done
        if len2 == 0 {
            break;
        }

        // Another iteration of the loop with odd and even swapped
        gf2_matrix_square(&mut odd, &even);
        if len2 & 1 != 0 {
            crc1n = gf2_matrix_times(&odd, crc1n);
        }
        len2 >>= 1;

        // If no more bits set, then done
        if len2 == 0 {
            break;
        }
    }

    // Return combined crc
    crc1n ^ crc2
}

#[cfg(test)]
mod tests {
    use super::{Checksum, ChecksumType};

    #[test]
    fn crc64_nvme_add_part_matches_full_object_checksum() {
        let data = (0..200_000).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let split_at = 73_421;
        let (first, second) = data.split_at(split_at);

        let expected = Checksum::new_from_data(ChecksumType::CRC64_NVME, &data).expect("full checksum");
        let first_checksum = Checksum::new_from_data(ChecksumType::CRC64_NVME, first).expect("first checksum");
        let second_checksum = Checksum::new_from_data(ChecksumType::CRC64_NVME, second).expect("second checksum");

        let mut combined = Checksum {
            checksum_type: ChecksumType::CRC64_NVME,
            ..Default::default()
        };
        combined
            .add_part(&first_checksum, first.len() as i64)
            .expect("add first part");
        combined
            .add_part(&second_checksum, second.len() as i64)
            .expect("add second part");

        assert_eq!(combined.encoded, expected.encoded);
        assert_eq!(combined.raw, expected.raw);
    }

    #[test]
    fn crc32c_add_part_matches_full_object_checksum() {
        let data = (0..32_768).map(|i| (255 - (i % 251)) as u8).collect::<Vec<_>>();
        let (first, rest) = data.split_at(7_777);
        let (second, third) = rest.split_at(13_333);

        let expected = Checksum::new_from_data(ChecksumType::CRC32C, &data).expect("full checksum");
        let first_checksum = Checksum::new_from_data(ChecksumType::CRC32C, first).expect("first checksum");
        let second_checksum = Checksum::new_from_data(ChecksumType::CRC32C, second).expect("second checksum");
        let third_checksum = Checksum::new_from_data(ChecksumType::CRC32C, third).expect("third checksum");

        let mut combined = Checksum {
            checksum_type: ChecksumType::CRC32C,
            ..Default::default()
        };
        combined
            .add_part(&first_checksum, first.len() as i64)
            .expect("add first part");
        combined
            .add_part(&second_checksum, second.len() as i64)
            .expect("add second part");
        combined
            .add_part(&third_checksum, third.len() as i64)
            .expect("add third part");

        assert_eq!(combined.encoded, expected.encoded);
        assert_eq!(combined.raw, expected.raw);
    }

    // Guardrail (#1254): every base algorithm must round-trip through EVERY dispatch
    // site. This fails loudly if a future algorithm is added to the enum but a match
    // arm (base/is_set/hasher/key/raw_byte_len/Display/from_string) or BASE_TYPE_MASK
    // is forgotten — the silent-drop footgun that reintroduced #4800.
    #[test]
    fn base_checksum_types_round_trip_all_dispatch_sites() {
        use super::BASE_CHECKSUM_TYPES;
        use std::io::Write;

        for &t in BASE_CHECKSUM_TYPES {
            assert_eq!(t.base(), t, "base() stripped {t:?}; BASE_TYPE_MASK is missing its bit");
            assert!(t.is_set(), "{t:?} is not is_set()");
            assert!(t.hasher().is_some(), "{t:?} has no hasher()");
            assert!(t.key().is_some(), "{t:?} has no header key()");
            assert!(t.raw_byte_len() > 0, "{t:?} has zero raw_byte_len()");

            let name = t.to_string();
            assert_ne!(name, "invalid", "{t:?} Display returns \"invalid\"");
            assert_ne!(name, "", "{t:?} Display returns empty");
            assert_eq!(
                ChecksumType::from_string(&name).base(),
                t,
                "from_string({name}) does not round-trip to {t:?}"
            );

            let mut hasher = t.hasher().expect("hasher present");
            hasher.write_all(b"rustfs checksum round-trip").expect("write");
            assert_eq!(
                hasher.finalize().len(),
                t.raw_byte_len(),
                "{t:?} finalized digest length != raw_byte_len()"
            );
        }
    }

    // The new AWS 2026-04 algorithms are COMPOSITE-only: an explicit FULL_OBJECT
    // request must be rejected (they cannot be linearly combined like CRCs), and they
    // must never be routed through add_part()/can_merge().
    #[test]
    fn new_algorithms_are_composite_only() {
        for alg in ["XXHASH3", "XXHASH64", "XXHASH128", "SHA512", "MD5"] {
            let composite = ChecksumType::from_string_with_obj_type(alg, "COMPOSITE");
            assert!(composite.is_set(), "{alg} COMPOSITE should be valid");
            assert!(!composite.can_merge(), "{alg} must not be mergeable (composite-only)");

            let full = ChecksumType::from_string_with_obj_type(alg, "FULL_OBJECT");
            assert_eq!(full, ChecksumType::INVALID, "{alg} FULL_OBJECT must be rejected");
        }
    }

    // Known-answer vectors (#1255). The empty-input digests are the OFFICIAL upstream
    // xxHash / SHA-512 test vectors (seed 0), pinned in big-endian to guarantee the
    // stored/echoed value byte-for-byte matches what AWS SDKs (awscrt) compute. If the
    // finalize() byte order or seed ever drifts, these fail loudly.
    fn raw_hex(t: ChecksumType, data: &[u8]) -> String {
        let c = Checksum::new_from_data(t, data).expect("checksum");
        assert_eq!(c.raw.len(), t.raw_byte_len(), "raw len mismatch for {t:?}");
        c.raw.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn xxhash_sha512_known_answer_vectors_empty_input() {
        // XXH3-64("") = 0x2D06800538D394C2 (official)
        assert_eq!(raw_hex(ChecksumType::XXHASH3, b""), "2d06800538d394c2");
        // XXH64("")   = 0xEF46DB3751D8E999 (official)
        assert_eq!(raw_hex(ChecksumType::XXHASH64, b""), "ef46db3751d8e999");
        // XXH3-128("") = 0x99AA06D3014798D86001C324468D497F (official)
        assert_eq!(raw_hex(ChecksumType::XXHASH128, b""), "99aa06d3014798d86001c324468d497f");
        // SHA-512("") (official)
        assert_eq!(
            raw_hex(ChecksumType::SHA512, b""),
            "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce\
             47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e"
        );
        // MD5("") = d41d8cd98f00b204e9800998ecf8427e (official)
        assert_eq!(raw_hex(ChecksumType::MD5, b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    // Regression lock for a non-empty payload: values are produced by this
    // implementation and must stay stable across refactors. Base64 (S3 wire form) is
    // asserted alongside the raw hex so both the digest and its encoding are pinned.
    #[test]
    fn xxhash_sha512_regression_lock_non_empty() {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let data = b"The quick brown fox jumps over the lazy dog";

        // XXH3-64(fox) = 0xce7d19a5418fb365 is the official upstream vector.
        for (t, want_hex) in [
            (ChecksumType::XXHASH3, "ce7d19a5418fb365"),
            (ChecksumType::XXHASH64, "0b242d361fda71bc"),
        ] {
            let c = Checksum::new_from_data(t, data).expect("checksum");
            let got_hex: String = c.raw.iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(got_hex, want_hex, "{t:?} raw hex drifted");
            // encoded field must be the standard-base64 of raw (S3 wire form)
            assert_eq!(c.encoded, STANDARD.encode(&c.raw), "{t:?} encoded field != base64(raw)");
        }
    }

    // On-disk (xl.meta) serialization round-trip for the new algorithms (#1260):
    // to_bytes() -> read_checksums() must recover the value under the Display key.
    #[test]
    fn on_disk_round_trip_new_algorithms() {
        use super::read_checksums;
        let data = b"rustfs on-disk checksum round-trip payload";

        for (t, name) in [
            (ChecksumType::XXHASH3, "XXHASH3"),
            (ChecksumType::XXHASH64, "XXHASH64"),
            (ChecksumType::XXHASH128, "XXHASH128"),
            (ChecksumType::SHA512, "SHA512"),
        ] {
            let c = Checksum::new_from_data(t, data).expect("checksum");
            let buf = c.to_bytes(&[]);
            let (map, is_multipart) = read_checksums(&buf, 0);
            assert!(!is_multipart, "{name} single-object must not be multipart");
            assert_eq!(map.get(name), Some(&c.encoded), "{name} did not round-trip on-disk");
        }
    }

    // Forward-compat / rolling-upgrade contract (#1260): a node that does not know a
    // future base-type bit must DEGRADE SAFELY — skip the entry and return without
    // panicking or decoding a wrong length — never crash or corrupt.
    #[test]
    fn unknown_future_type_bit_degrades_safely() {
        use super::{encode_varint, read_checksums, read_part_checksums};

        // A base-type bit far above any allocated one (append-only rule guarantees
        // real bits stay below this), followed by 8 bytes of would-be digest.
        let mut buf = Vec::new();
        encode_varint(&mut buf, 1 << 20);
        buf.extend_from_slice(&[0xAB; 8]);

        let (map, is_multipart) = read_checksums(&buf, 0);
        assert!(map.is_empty(), "unknown type must yield no checksum, got {map:?}");
        assert!(!is_multipart);
        assert!(read_part_checksums(&buf).is_empty());
    }

    // Multipart COMPOSITE assembly for the composite-only algorithms (#1261). Mirrors
    // set_disk complete_multipart_upload: the object checksum is H(concat of per-part
    // raw digests), and these algorithms must NOT be routed through add_part().
    #[test]
    fn composite_multipart_assembly_new_algorithms() {
        let part1 = b"first multipart chunk bytes";
        let part2 = b"second multipart chunk bytes";

        for t in [
            ChecksumType::XXHASH3,
            ChecksumType::XXHASH64,
            ChecksumType::XXHASH128,
            ChecksumType::SHA512,
            ChecksumType::MD5,
        ] {
            let c1 = Checksum::new_from_data(t, part1).expect("part1");
            let c2 = Checksum::new_from_data(t, part2).expect("part2");

            // Concatenate raw part digests, then hash again with the same algorithm.
            let mut combined = c1.raw.clone();
            combined.extend_from_slice(&c2.raw);
            let composite = Checksum::new_from_data(t, &combined).expect("composite");
            assert_eq!(composite.raw.len(), t.raw_byte_len(), "{t:?} composite len");
            assert!(!composite.encoded.is_empty());

            // Composite-only: full-object merge must be refused.
            let mut acc = Checksum {
                checksum_type: t,
                ..Default::default()
            };
            assert!(acc.add_part(&c1, part1.len() as i64).is_err(), "{t:?} must not be full-object mergeable");
        }
    }

    // S11: from_string_with_obj_type dropped to_uppercase() (a per-request heap alloc)
    // for eq_ignore_ascii_case. Lock that this did not change any behaviour.
    #[test]
    fn from_string_is_case_insensitive_and_behaviour_preserved() {
        assert_eq!(ChecksumType::from_string("crc32").base(), ChecksumType::CRC32);
        assert_eq!(ChecksumType::from_string("Crc32C").base(), ChecksumType::CRC32C);
        assert_eq!(ChecksumType::from_string("xxHASH3").base(), ChecksumType::XXHASH3);
        assert_eq!(ChecksumType::from_string("Md5").base(), ChecksumType::MD5);
        assert_eq!(ChecksumType::from_string("sha512").base(), ChecksumType::SHA512);

        // Unknown / empty preserved.
        assert_eq!(ChecksumType::from_string("nope"), ChecksumType::INVALID);
        assert_eq!(ChecksumType::from_string(""), ChecksumType::NONE);

        // CRC64NVME still assumes full-object; CRC32 still accepts explicit FULL_OBJECT.
        assert!(ChecksumType::from_string("crc64nvme").full_object_requested());
        assert!(ChecksumType::from_string_with_obj_type("crc32", "FULL_OBJECT").full_object_requested());

        // Composite-only algorithms still reject FULL_OBJECT; invalid obj_type still rejected.
        assert_eq!(ChecksumType::from_string_with_obj_type("xxhash3", "FULL_OBJECT"), ChecksumType::INVALID);
        assert_eq!(ChecksumType::from_string_with_obj_type("crc32", "bogus"), ChecksumType::INVALID);
    }

    // is_s3s_typed is the single source of truth for the "five typed fields" vs
    // "additional algorithms carried as raw headers" split used across the handlers.
    #[test]
    fn is_s3s_typed_split_is_exhaustive() {
        for t in [
            ChecksumType::CRC32,
            ChecksumType::CRC32C,
            ChecksumType::SHA1,
            ChecksumType::SHA256,
            ChecksumType::CRC64_NVME,
        ] {
            assert!(t.is_s3s_typed(), "{t:?} must be s3s-typed");
        }
        for t in [
            ChecksumType::XXHASH3,
            ChecksumType::XXHASH64,
            ChecksumType::XXHASH128,
            ChecksumType::SHA512,
            ChecksumType::MD5,
        ] {
            assert!(!t.is_s3s_typed(), "{t:?} must NOT be s3s-typed (additional algorithm)");
        }
        assert!(!ChecksumType::NONE.is_s3s_typed());
        // Flags on top of a base type must not change the classification.
        let crc32_full = ChecksumType(ChecksumType::CRC32.0 | ChecksumType::FULL_OBJECT.0);
        assert!(crc32_full.is_s3s_typed());
        let xxh3_multipart = ChecksumType(ChecksumType::XXHASH3.0 | ChecksumType::MULTIPART.0);
        assert!(!xxh3_multipart.is_s3s_typed());
    }
}
