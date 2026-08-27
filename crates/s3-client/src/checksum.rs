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

use rustfs_checksums::ChecksumAlgorithm;
use std::collections::HashMap;

use crate::utils::base64_encode;
use crate::{api_put_object::PutObjectOptions, api_s3_datatypes::ObjectPart};

use enumset::EnumSetType;

/// The MinIO-port client's checksum vocabulary: the standard S3 algorithm set
/// plus the `None`/`FullObject` markers this client's option plumbing needs
/// (the RustFS extension algorithms do not exist on this client path). All
/// per-algorithm dispatch — header names, wire names, digest lengths,
/// checksum-type capabilities, hashers — is delegated through [`Self::algorithm`]
/// to the canonical registry in `rustfs_checksums::ChecksumAlgorithm`
/// (crates/checksums/src/lib.rs); only the variant-to-algorithm bridge lives
/// here. The on-disk xl.meta bitset remains separate in
/// `rustfs_rio::ChecksumType` (crates/rio/src/checksum.rs, varint bits are
/// append-only). When adding an algorithm: extend `ChecksumAlgorithm` (its
/// exhaustive matches force the metadata), add the variant + one `algorithm()`
/// arm here, and allocate an xl.meta bit in rio (or record why not).
#[derive(Debug, EnumSetType, Default)]
#[enumset(repr = "u8")]
pub enum ChecksumMode {
    #[default]
    ChecksumNone,
    ChecksumSHA256,
    ChecksumSHA1,
    ChecksumCRC32,
    ChecksumCRC32C,
    ChecksumCRC64NVME,
    ChecksumFullObject,
}

impl ChecksumMode {
    /// The single bridge from this client vocabulary to the canonical
    /// algorithm registry. Every per-algorithm question below goes through
    /// here; the marker variants (`ChecksumNone`, bare `ChecksumFullObject`)
    /// map to `None` and fail closed at each call site.
    pub fn algorithm(&self) -> Option<ChecksumAlgorithm> {
        match self {
            ChecksumMode::ChecksumSHA256 => Some(ChecksumAlgorithm::Sha256),
            ChecksumMode::ChecksumSHA1 => Some(ChecksumAlgorithm::Sha1),
            ChecksumMode::ChecksumCRC32 => Some(ChecksumAlgorithm::Crc32),
            ChecksumMode::ChecksumCRC32C => Some(ChecksumAlgorithm::Crc32c),
            ChecksumMode::ChecksumCRC64NVME => Some(ChecksumAlgorithm::Crc64Nvme),
            ChecksumMode::ChecksumNone | ChecksumMode::ChecksumFullObject => None,
        }
    }

    pub fn base(&self) -> ChecksumMode {
        // Fail closed: any mode without a concrete base algorithm (e.g. a
        // bare ChecksumFullObject flag) is treated as "no checksum" rather
        // than panicking. Callers already gate real work behind
        // is_set()/can_composite()/hasher(), so this only removes a crash.
        if self.algorithm().is_some() {
            *self
        } else {
            ChecksumMode::ChecksumNone
        }
    }

    pub fn is(&self, t: ChecksumMode) -> bool {
        *self & t == t
    }

    pub fn key(&self) -> String {
        self.algorithm().map(|a| a.http_header_name().to_string()).unwrap_or_default()
    }

    pub fn can_composite(&self) -> bool {
        self.algorithm().is_some_and(|a| a.supports_composite())
    }

    pub fn can_merge_crc(&self) -> bool {
        self.algorithm().is_some_and(|a| a.supports_full_object())
    }

    pub fn full_object_requested(&self) -> bool {
        // CRC64NVME is FULL_OBJECT-only, so selecting it implies a full-object
        // checksum even without an explicit request (AWS behaviour).
        self.algorithm()
            .is_some_and(|a| a.supports_full_object() && !a.supports_composite())
    }

    pub fn raw_byte_len(&self) -> usize {
        self.algorithm().map(|a| a.raw_len()).unwrap_or(0)
    }

    pub fn hasher(&self) -> Result<Box<dyn rustfs_checksums::http::HttpChecksum>, std::io::Error> {
        self.algorithm()
            .map(ChecksumAlgorithm::into_impl)
            .ok_or_else(|| std::io::Error::other("unsupported checksum type"))
    }

    pub fn is_set(&self) -> bool {
        // A checksum is only "set" when a concrete algorithm (one with a real
        // hasher) is selected; `ChecksumNone` and the bare `ChecksumFullObject`
        // flag are not. Treating `ChecksumNone` as set made ILM transitions of
        // >128 MiB objects fail with "unsupported checksum type"
        // (rustfs/rustfs#4811): the multipart put path took the checksum branch
        // and called `ChecksumNone.hasher()`.
        self.algorithm().is_some()
    }

    pub fn set_default(&mut self, t: ChecksumMode) {
        if !self.is_set() {
            *self = t;
        }
    }

    pub fn encode_to_string(&self, b: &[u8]) -> Result<String, std::io::Error> {
        if !self.is_set() {
            return Ok("".to_string());
        }
        let mut h = self.hasher()?;
        h.update(b);
        let hash = h.finalize();
        Ok(base64_encode(hash.as_ref()))
    }

    pub fn composite_checksum(&self, p: &mut [ObjectPart]) -> Result<Checksum, std::io::Error> {
        if !self.can_composite() {
            return Err(std::io::Error::other("cannot do composite checksum"));
        }
        p.sort_by_key(|part| part.part_num);
        let c = self.base();
        let mut crc_bytes = Vec::<u8>::with_capacity(p.len() * self.raw_byte_len());
        let mut h = self.hasher()?;
        for part in p.iter() {
            let part_checksum = part.checksum_raw(&c)?;
            crc_bytes.extend(part_checksum);
        }
        h.update(crc_bytes.as_ref());
        let hash = h.finalize();
        Ok(Checksum {
            checksum_type: *self,
            r: hash.as_ref().to_vec(),
        })
    }

    pub fn full_object_checksum(&self, p: &mut [ObjectPart]) -> Result<Checksum, std::io::Error> {
        if !self.can_merge_crc() {
            return Err(std::io::Error::other("cannot do full-object checksum"));
        }

        self.composite_checksum(p)
    }
}

impl std::fmt::Display for ChecksumMode {
    /// The `x-amz-checksum-algorithm` wire value: the algorithm name for
    /// concrete modes, `""` for `ChecksumNone`, `"<invalid>"` for a bare
    /// `ChecksumFullObject` flag.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.algorithm() {
            Some(algorithm) => f.write_str(algorithm.s3_algorithm_name()),
            None if matches!(self, ChecksumMode::ChecksumNone) => Ok(()),
            None => f.write_str("<invalid>"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ChecksumMode;

    #[test]
    fn test_base_is_fail_closed_and_never_panics() {
        // Every mode must resolve to a concrete base without panicking. The bare
        // ChecksumFullObject flag has no base algorithm and must fall back to
        // ChecksumNone instead of crashing (previously `panic!("enum err.")`).
        assert_eq!(ChecksumMode::ChecksumFullObject.base(), ChecksumMode::ChecksumNone);
        assert_eq!(ChecksumMode::ChecksumNone.base(), ChecksumMode::ChecksumNone);
        assert_eq!(ChecksumMode::ChecksumCRC32.base(), ChecksumMode::ChecksumCRC32);
        assert_eq!(ChecksumMode::ChecksumCRC32C.base(), ChecksumMode::ChecksumCRC32C);
        assert_eq!(ChecksumMode::ChecksumSHA1.base(), ChecksumMode::ChecksumSHA1);
        assert_eq!(ChecksumMode::ChecksumSHA256.base(), ChecksumMode::ChecksumSHA256);
        assert_eq!(ChecksumMode::ChecksumCRC64NVME.base(), ChecksumMode::ChecksumCRC64NVME);
    }

    #[test]
    fn test_hasher_fails_closed_for_unsupported_mode() {
        // Modes without a real hasher must return an error, not panic.
        assert!(ChecksumMode::ChecksumNone.hasher().is_err());
        assert!(ChecksumMode::ChecksumFullObject.hasher().is_err());
        assert!(ChecksumMode::ChecksumCRC32.hasher().is_ok());
    }

    #[test]
    fn test_is_set_is_false_for_none_and_bare_full_object() {
        // Regression for rustfs/rustfs#4811: `ChecksumNone` must NOT be reported as
        // a configured checksum. It is the zeroth enum variant (bit 0 of the
        // EnumSet repr), so the old `len() == 1` check treated it as set and drove
        // the multipart put path into `ChecksumNone.hasher()` → "unsupported
        // checksum type". Every mode reported as set must also have a real hasher.
        assert!(!ChecksumMode::ChecksumNone.is_set());
        assert!(!ChecksumMode::ChecksumFullObject.is_set());

        for mode in [
            ChecksumMode::ChecksumCRC32,
            ChecksumMode::ChecksumCRC32C,
            ChecksumMode::ChecksumSHA1,
            ChecksumMode::ChecksumSHA256,
            ChecksumMode::ChecksumCRC64NVME,
        ] {
            assert!(mode.is_set(), "{mode:?} should be set");
            assert!(mode.hasher().is_ok(), "{mode:?} reported set but has no hasher");
        }
    }

    #[test]
    fn test_delegated_dispatch_preserves_wire_behaviour() {
        // Behaviour lock for the backlog#1844 unification: every output that
        // reaches the wire (header names, algorithm names, digest lengths,
        // checksum-type capabilities) is pinned to the exact values the
        // pre-delegation per-algorithm matches produced. If delegation to
        // rustfs_checksums::ChecksumAlgorithm ever drifts, this fails loudly.
        struct Expected {
            mode: ChecksumMode,
            key: &'static str,
            name: &'static str,
            raw_len: usize,
            composite: bool,
            merge_crc: bool,
            full_object: bool,
        }
        let table = [
            Expected {
                mode: ChecksumMode::ChecksumCRC32,
                key: "x-amz-checksum-crc32",
                name: "CRC32",
                raw_len: 4,
                composite: true,
                merge_crc: true,
                full_object: false,
            },
            Expected {
                mode: ChecksumMode::ChecksumCRC32C,
                key: "x-amz-checksum-crc32c",
                name: "CRC32C",
                raw_len: 4,
                composite: true,
                merge_crc: true,
                full_object: false,
            },
            Expected {
                mode: ChecksumMode::ChecksumSHA1,
                key: "x-amz-checksum-sha1",
                name: "SHA1",
                raw_len: 20,
                composite: true,
                merge_crc: false,
                full_object: false,
            },
            Expected {
                mode: ChecksumMode::ChecksumSHA256,
                key: "x-amz-checksum-sha256",
                name: "SHA256",
                raw_len: 32,
                composite: true,
                merge_crc: false,
                full_object: false,
            },
            Expected {
                mode: ChecksumMode::ChecksumCRC64NVME,
                key: "x-amz-checksum-crc64nvme",
                name: "CRC64NVME",
                raw_len: 8,
                composite: false,
                merge_crc: true,
                full_object: true,
            },
            Expected {
                mode: ChecksumMode::ChecksumNone,
                key: "",
                name: "",
                raw_len: 0,
                composite: false,
                merge_crc: false,
                full_object: false,
            },
            Expected {
                mode: ChecksumMode::ChecksumFullObject,
                key: "",
                name: "<invalid>",
                raw_len: 0,
                composite: false,
                merge_crc: false,
                full_object: false,
            },
        ];

        for e in table {
            assert_eq!(e.mode.key(), e.key, "{:?} key()", e.mode);
            assert_eq!(e.mode.to_string(), e.name, "{:?} to_string()", e.mode);
            assert_eq!(e.mode.raw_byte_len(), e.raw_len, "{:?} raw_byte_len()", e.mode);
            assert_eq!(e.mode.can_composite(), e.composite, "{:?} can_composite()", e.mode);
            assert_eq!(e.mode.can_merge_crc(), e.merge_crc, "{:?} can_merge_crc()", e.mode);
            assert_eq!(e.mode.full_object_requested(), e.full_object, "{:?} full_object_requested()", e.mode);

            // The hasher, when present, must produce digests of the advertised
            // length under the advertised header name.
            if let Ok(mut hasher) = e.mode.hasher() {
                assert_eq!(e.mode.hasher().unwrap().header_name(), e.key, "{:?} hasher header", e.mode);
                hasher.update(b"wire behaviour probe");
                assert_eq!(hasher.finalize().len(), e.raw_len, "{:?} digest length", e.mode);
            } else {
                assert_eq!(e.raw_len, 0, "{:?} has no hasher but a nonzero raw_len", e.mode);
            }
        }
    }

    #[test]
    fn test_checksum_header_value_reads_present_absent_and_invalid() {
        use super::checksum_header_value;
        use http::{HeaderMap, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-checksum-crc32c", HeaderValue::from_static("yZRlqg=="));
        headers.insert("x-amz-checksum-sha256", HeaderValue::from_bytes(b"\xff\xfe").unwrap());

        // Present header returns its value verbatim.
        assert_eq!(checksum_header_value(&headers, ChecksumMode::ChecksumCRC32C), "yZRlqg==");
        // Absent header and the unset modes (whose key() is "") return "".
        assert_eq!(checksum_header_value(&headers, ChecksumMode::ChecksumCRC32), "");
        assert_eq!(checksum_header_value(&headers, ChecksumMode::ChecksumNone), "");
        // A non-UTF-8 header value degrades to "" instead of erroring.
        assert_eq!(checksum_header_value(&headers, ChecksumMode::ChecksumSHA256), "");
    }

    #[test]
    fn test_set_default_upgrades_none() {
        // With `is_set()` fixed, `set_default` must upgrade an unset mode to the
        // provided default (previously `ChecksumNone` was seen as set and never
        // upgraded).
        let mut mode = ChecksumMode::ChecksumNone;
        mode.set_default(ChecksumMode::ChecksumCRC32C);
        assert_eq!(mode, ChecksumMode::ChecksumCRC32C);

        // An already-set mode is left untouched.
        let mut existing = ChecksumMode::ChecksumSHA256;
        existing.set_default(ChecksumMode::ChecksumCRC32C);
        assert_eq!(existing, ChecksumMode::ChecksumSHA256);
    }
}

#[derive(Default)]
pub struct Checksum {
    checksum_type: ChecksumMode,
    r: Vec<u8>,
}

impl Checksum {
    fn is_set(&self) -> bool {
        self.checksum_type.is_set() && self.r.len() == self.checksum_type.raw_byte_len()
    }

    fn encoded(&self) -> String {
        if !self.is_set() {
            return "".to_string();
        }
        base64_encode(&self.r)
    }
}

/// Read the base64 digest carried by `mode`'s `x-amz-checksum-*` response
/// header, or an empty string when the header is absent (the client's
/// datatypes use `""` for "no checksum").
pub fn checksum_header_value(headers: &http::HeaderMap, mode: ChecksumMode) -> String {
    headers
        .get(mode.key())
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string()
}

pub fn add_auto_checksum_headers(opts: &mut PutObjectOptions) {
    opts.user_metadata
        .insert("X-Amz-Checksum-Algorithm".to_string(), opts.auto_checksum.to_string());
    if opts.auto_checksum.full_object_requested() {
        opts.user_metadata
            .insert("X-Amz-Checksum-Type".to_string(), "FULL_OBJECT".to_string());
    }
}

pub fn apply_auto_checksum(opts: &mut PutObjectOptions, all_parts: &mut [ObjectPart]) -> Result<(), std::io::Error> {
    if opts.auto_checksum.can_composite() && !opts.auto_checksum.is(ChecksumMode::ChecksumFullObject) {
        let crc = opts.auto_checksum.composite_checksum(all_parts)?;
        opts.user_metadata = {
            let mut hm = HashMap::new();
            hm.insert(opts.auto_checksum.key(), crc.encoded());
            hm
        }
    } else if opts.auto_checksum.can_merge_crc() {
        let crc = opts.auto_checksum.full_object_checksum(all_parts)?;
        opts.user_metadata = {
            let mut hm = HashMap::new();
            hm.insert(opts.auto_checksum.key(), crc.encoded());
            hm.insert("X-Amz-Checksum-Type".to_string(), "FULL_OBJECT".to_string());
            hm
        }
    }

    Ok(())
}
