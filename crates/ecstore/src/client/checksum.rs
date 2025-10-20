#![allow(clippy::map_entry)]
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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]
#![allow(unused_must_use)]
#![allow(clippy::all)]

use lazy_static::lazy_static;
use rustfs_checksums::ChecksumAlgorithm;
use std::collections::HashMap;

use crate::client::utils::base64_decode;
use crate::client::utils::base64_encode;
use crate::client::{api_put_object::PutObjectOptions, api_s3_datatypes::ObjectPart};
use crate::{disk::DiskAPI, store_api::GetObjectReader};
use s3s::header::{
    X_AMZ_CHECKSUM_ALGORITHM, X_AMZ_CHECKSUM_CRC32, X_AMZ_CHECKSUM_CRC32C, X_AMZ_CHECKSUM_SHA1, X_AMZ_CHECKSUM_SHA256,
};

use enumset::{EnumSet, EnumSetType, enum_set};

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

lazy_static! {
    static ref C_ChecksumMask: EnumSet<ChecksumMode> = {
        let mut s = EnumSet::all();
        s.remove(ChecksumMode::ChecksumFullObject);
        s
    };
    static ref C_ChecksumFullObjectCRC32: EnumSet<ChecksumMode> =
        enum_set!(ChecksumMode::ChecksumCRC32 | ChecksumMode::ChecksumFullObject);
    static ref C_ChecksumFullObjectCRC32C: EnumSet<ChecksumMode> =
        enum_set!(ChecksumMode::ChecksumCRC32C | ChecksumMode::ChecksumFullObject);
}
const AMZ_CHECKSUM_CRC64NVME: &str = "x-amz-checksum-crc64nvme";

impl ChecksumMode {
    //pub const CRC64_NVME_POLYNOMIAL: i64 = 0xad93d23594c93659;

    pub fn base(&self) -> ChecksumMode {
        let s = EnumSet::from(*self).intersection(*C_ChecksumMask);
        match s.as_u8() {
            1_u8 => ChecksumMode::ChecksumNone,
            2_u8 => ChecksumMode::ChecksumSHA256,
            4_u8 => ChecksumMode::ChecksumSHA1,
            8_u8 => ChecksumMode::ChecksumCRC32,
            16_u8 => ChecksumMode::ChecksumCRC32C,
            32_u8 => ChecksumMode::ChecksumCRC64NVME,
            _ => panic!("enum err."),
        }
    }

    pub fn is(&self, t: ChecksumMode) -> bool {
        *self & t == t
    }

    pub fn key(&self) -> String {
        //match c & checksumMask {
        match self {
            ChecksumMode::ChecksumCRC32 => {
                return X_AMZ_CHECKSUM_CRC32.to_string();
            }
            ChecksumMode::ChecksumCRC32C => {
                return X_AMZ_CHECKSUM_CRC32C.to_string();
            }
            ChecksumMode::ChecksumSHA1 => {
                return X_AMZ_CHECKSUM_SHA1.to_string();
            }
            ChecksumMode::ChecksumSHA256 => {
                return X_AMZ_CHECKSUM_SHA256.to_string();
            }
            ChecksumMode::ChecksumCRC64NVME => {
                return AMZ_CHECKSUM_CRC64NVME.to_string();
            }
            _ => {
                return "".to_string();
            }
        }
    }

    pub fn can_composite(&self) -> bool {
        let s = EnumSet::from(*self).intersection(*C_ChecksumMask);
        match s.as_u8() {
            2_u8 => true,
            4_u8 => true,
            8_u8 => true,
            16_u8 => true,
            _ => false,
        }
    }

    pub fn can_merge_crc(&self) -> bool {
        let s = EnumSet::from(*self).intersection(*C_ChecksumMask);
        match s.as_u8() {
            8_u8 => true,
            16_u8 => true,
            32_u8 => true,
            _ => false,
        }
    }

    pub fn full_object_requested(&self) -> bool {
        let s = EnumSet::from(*self).intersection(*C_ChecksumMask);
        match s.as_u8() {
            //C_ChecksumFullObjectCRC32 as u8 => true,
            //C_ChecksumFullObjectCRC32C as u8 => true,
            32_u8 => true,
            _ => false,
        }
    }

    pub fn key_capitalized(&self) -> String {
        self.key()
    }

    pub fn raw_byte_len(&self) -> usize {
        let u = EnumSet::from(*self).intersection(*C_ChecksumMask).as_u8();
        if u == ChecksumMode::ChecksumCRC32 as u8 || u == ChecksumMode::ChecksumCRC32C as u8 {
            4
        } else if u == ChecksumMode::ChecksumSHA1 as u8 {
            use sha1::Digest;
            sha1::Sha1::output_size() as usize
        } else if u == ChecksumMode::ChecksumSHA256 as u8 {
            use sha2::Digest;
            sha2::Sha256::output_size() as usize
        } else if u == ChecksumMode::ChecksumCRC64NVME as u8 {
            8
        } else {
            0
        }
    }

    pub fn hasher(&self) -> Result<Box<dyn rustfs_checksums::http::HttpChecksum>, std::io::Error> {
        match /*C_ChecksumMask & **/self {
            ChecksumMode::ChecksumCRC32 => {
                return Ok(ChecksumAlgorithm::Crc32.into_impl());
            }
            ChecksumMode::ChecksumCRC32C => {
                return Ok(ChecksumAlgorithm::Crc32c.into_impl());
            }
            ChecksumMode::ChecksumSHA1 => {
                return Ok(ChecksumAlgorithm::Sha1.into_impl());
            }
            ChecksumMode::ChecksumSHA256 => {
                return Ok(ChecksumAlgorithm::Sha256.into_impl());
            }
            ChecksumMode::ChecksumCRC64NVME => {
                return Ok(ChecksumAlgorithm::Crc64Nvme.into_impl());
            }
            _ => return Err(std::io::Error::other("unsupported checksum type")),
        }
    }

    pub fn is_set(&self) -> bool {
        let s = EnumSet::from(*self).intersection(*C_ChecksumMask);
        s.len() == 1
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

    pub fn to_string(&self) -> String {
        //match c & checksumMask {
        match self {
            ChecksumMode::ChecksumCRC32 => {
                return "CRC32".to_string();
            }
            ChecksumMode::ChecksumCRC32C => {
                return "CRC32C".to_string();
            }
            ChecksumMode::ChecksumSHA1 => {
                return "SHA1".to_string();
            }
            ChecksumMode::ChecksumSHA256 => {
                return "SHA256".to_string();
            }
            ChecksumMode::ChecksumNone => {
                return "".to_string();
            }
            ChecksumMode::ChecksumCRC64NVME => {
                return "CRC64NVME".to_string();
            }
            _ => {
                return "<invalid>".to_string();
            }
        }
    }

    // pub fn check_sum_reader(&self, r: GetObjectReader) -> Result<Checksum, std::io::Error> {
    //     let mut h = self.hasher()?;
    //     Ok(Checksum::new(self.clone(), h.sum().as_bytes()))
    // }

    // pub fn check_sum_bytes(&self, b: &[u8]) -> Result<Checksum, std::io::Error> {
    //     let mut h = self.hasher()?;
    //     Ok(Checksum::new(self.clone(), h.sum().as_bytes()))
    // }

    pub fn composite_checksum(&self, p: &mut [ObjectPart]) -> Result<Checksum, std::io::Error> {
        if !self.can_composite() {
            return Err(std::io::Error::other("cannot do composite checksum"));
        }
        p.sort_by(|i, j| {
            if i.part_num < j.part_num {
                std::cmp::Ordering::Less
            } else if i.part_num > j.part_num {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        let c = self.base();
        let crc_bytes = Vec::<u8>::with_capacity(p.len() * self.raw_byte_len() as usize);
        let mut h = self.hasher()?;
        h.update(crc_bytes.as_ref());
        let hash = h.finalize();
        Ok(Checksum {
            checksum_type: self.clone(),
            r: hash.as_ref().to_vec(),
            computed: false,
        })
    }

    pub fn full_object_checksum(&self, p: &mut [ObjectPart]) -> Result<Checksum, std::io::Error> {
        todo!();
    }
}

#[derive(Default)]
pub struct Checksum {
    checksum_type: ChecksumMode,
    r: Vec<u8>,
    computed: bool,
}

#[allow(dead_code)]
impl Checksum {
    fn new(t: ChecksumMode, b: &[u8]) -> Checksum {
        if t.is_set() && b.len() == t.raw_byte_len() {
            return Checksum {
                checksum_type: t,
                r: b.to_vec(),
                computed: false,
            };
        }
        Checksum::default()
    }

    #[allow(dead_code)]
    fn new_checksum_string(t: ChecksumMode, s: &str) -> Result<Checksum, std::io::Error> {
        let b = match base64_decode(s.as_bytes()) {
            Ok(b) => b,
            Err(err) => return Err(std::io::Error::other(err.to_string())),
        };
        if t.is_set() && b.len() == t.raw_byte_len() {
            return Ok(Checksum {
                checksum_type: t,
                r: b,
                computed: false,
            });
        }
        Ok(Checksum::default())
    }

    fn is_set(&self) -> bool {
        self.checksum_type.is_set() && self.r.len() == self.checksum_type.raw_byte_len()
    }

    fn encoded(&self) -> String {
        if !self.is_set() {
            return "".to_string();
        }
        base64_encode(&self.r)
    }

    #[allow(dead_code)]
    fn raw(&self) -> Option<Vec<u8>> {
        if !self.is_set() {
            return None;
        }
        Some(self.r.clone())
    }
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
            hm.insert(opts.auto_checksum.key_capitalized(), crc.encoded());
            hm.insert("X-Amz-Checksum-Type".to_string(), "FULL_OBJECT".to_string());
            hm
        }
    }

    Ok(())
}
