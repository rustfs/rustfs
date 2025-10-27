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
use crc32fast::Hasher as Crc32Hasher;
use http::HeaderMap;
use sha1::Sha1;
use sha2::{Digest, Sha256};
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

    /// No checksum
    pub const NONE: ChecksumType = ChecksumType(0);

    const BASE_TYPE_MASK: u32 = Self::SHA256.0 | Self::SHA1.0 | Self::CRC32.0 | Self::CRC32C.0 | Self::CRC64_NVME.0;

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

        match alg.to_uppercase().as_str() {
            "CRC32" => ChecksumType(Self::CRC32.0 | full.0),
            "CRC32C" => ChecksumType(Self::CRC32C.0 | full.0),
            "SHA1" => {
                if full != Self::NONE {
                    return Self::INVALID;
                }
                Self::SHA1
            }
            "SHA256" => {
                if full != Self::NONE {
                    return Self::INVALID;
                }
                Self::SHA256
            }
            "CRC64NVME" => {
                // AWS seems to ignore full value and just assume it
                Self::CRC64_NVME
            }
            "" => {
                if full != Self::NONE {
                    return Self::INVALID;
                }
                Self::NONE
            }
            _ => Self::INVALID,
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
            Self::NONE => write!(f, ""),
            _ => write!(f, "invalid"),
        }
    }
}

/// Base checksum types list
pub const BASE_CHECKSUM_TYPES: &[ChecksumType] = &[
    ChecksumType::SHA256,
    ChecksumType::SHA1,
    ChecksumType::CRC32,
    ChecksumType::CRC64_NVME,
    ChecksumType::CRC32C,
];

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
            if raw_len == 0 || parts.len() % raw_len != 0 {
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
            for &checksum_type in crate::checksum::BASE_CHECKSUM_TYPES {
                if let Some(key) = checksum_type.key() {
                    if header.eq_ignore_ascii_case(key) {
                        duplicates = result.is_some();
                        result = Some(Checksum {
                            checksum_type: ChecksumType(checksum_type.0 | ChecksumType::TRAILING.0),
                            encoded: String::new(),
                            raw: Vec::new(),
                            want_parts: 0,
                        });
                    }
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

    let checksum = Checksum::new_with_type(checksum_type, &value);
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

        if checksum_type.is_set() {
            if let Some(key) = checksum_type.key() {
                if let Some(value) = headers.get(key).and_then(|v| v.to_str().ok()) {
                    return (checksum_type, value.to_string());
                } else {
                    return (ChecksumType::NONE, String::new());
                }
            }
        }
        return (checksum_type, String::new());
    }

    // Check individual checksum headers
    for &ct in crate::checksum::BASE_CHECKSUM_TYPES {
        if let Some(key) = ct.key() {
            if let Some(value) = headers.get(key).and_then(|v| v.to_str().ok()) {
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
    hasher: Crc32Hasher,
}

impl Default for Crc32IeeeHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32IeeeHasher {
    pub fn new() -> Self {
        Self {
            hasher: Crc32Hasher::new(),
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
        self.hasher.clone().finalize().to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = Crc32Hasher::new();
    }
}

/// CRC32 Castagnoli hasher
#[derive(Default)]
pub struct Crc32CastagnoliHasher(u32);

impl Crc32CastagnoliHasher {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Write for Crc32CastagnoliHasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 = crc32c::crc32c_append(self.0, buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Crc32CastagnoliHasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.0 = 0;
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
#[derive(Default)]
pub struct Crc64NvmeHasher {
    hasher: crc64fast_nvme::Digest,
}

impl Crc64NvmeHasher {
    pub fn new() -> Self {
        Self {
            hasher: Default::default(),
        }
    }
}

impl Write for Crc64NvmeHasher {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hasher.write(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ChecksumHasher for Crc64NvmeHasher {
    fn finalize(&mut self) -> Vec<u8> {
        self.hasher.sum64().to_be_bytes().to_vec()
    }

    fn reset(&mut self) {
        self.hasher = Default::default();
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
    let mut mat_iter = mat.iter();

    while vec != 0 {
        if vec & 1 != 0 {
            if let Some(&m) = mat_iter.next() {
                sum ^= m;
            }
        }
        vec >>= 1;
        mat_iter.next();
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
