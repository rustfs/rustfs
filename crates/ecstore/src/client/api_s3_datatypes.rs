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

use s3s::dto::Owner;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use time::OffsetDateTime;

use crate::client::checksum::ChecksumMode;
use crate::client::transition_api::ObjectMultipartInfo;
use crate::client::utils::base64_decode;

use super::transition_api;

pub struct ListAllMyBucketsResult {
    pub owner: Owner,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CommonPrefix {
    pub prefix: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
pub struct ListBucketV2Result {
    pub common_prefixes: Vec<CommonPrefix>,
    pub contents: Vec<transition_api::ObjectInfo>,
    pub delimiter: String,
    pub encoding_type: String,
    pub is_truncated: bool,
    pub max_keys: i64,
    pub name: String,
    pub next_continuation_token: String,
    pub continuation_token: String,
    pub prefix: String,
    pub fetch_owner: String,
    pub start_after: String,
}

#[allow(dead_code)]
pub struct Version {
    etag: String,
    is_latest: bool,
    key: String,
    last_modified: OffsetDateTime,
    owner: Owner,
    size: i64,
    storage_class: String,
    version_id: String,
    user_metadata: HashMap<String, String>,
    user_tags: HashMap<String, String>,
    is_delete_marker: bool,
}

pub struct ListVersionsResult {
    versions: Vec<Version>,
    common_prefixes: Vec<CommonPrefix>,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: i64,
    encoding_type: String,
    is_truncated: bool,
    key_marker: String,
    version_id_marker: String,
    next_key_marker: String,
    next_version_id_marker: String,
}

pub struct ListBucketResult {
    common_prefixes: Vec<CommonPrefix>,
    contents: Vec<transition_api::ObjectInfo>,
    delimiter: String,
    encoding_type: String,
    is_truncated: bool,
    marker: String,
    max_keys: i64,
    name: String,
    next_marker: String,
    prefix: String,
}

pub struct ListMultipartUploadsResult {
    bucket: String,
    key_marker: String,
    upload_id_marker: String,
    next_key_marker: String,
    next_upload_id_marker: String,
    encoding_type: String,
    max_uploads: i64,
    is_truncated: bool,
    uploads: Vec<ObjectMultipartInfo>,
    prefix: String,
    delimiter: String,
    common_prefixes: Vec<CommonPrefix>,
}

pub struct Initiator {
    id: String,
    display_name: String,
}

pub struct CopyObjectResult {
    pub etag: String,
    pub last_modified: OffsetDateTime,
}

#[derive(Debug, Clone)]
pub struct ObjectPart {
    pub etag: String,
    pub part_num: i64,
    pub last_modified: OffsetDateTime,
    pub size: i64,
    pub checksum_crc32: String,
    pub checksum_crc32c: String,
    pub checksum_sha1: String,
    pub checksum_sha256: String,
    pub checksum_crc64nvme: String,
}

impl Default for ObjectPart {
    fn default() -> Self {
        ObjectPart {
            etag: Default::default(),
            part_num: 0,
            last_modified: OffsetDateTime::now_utc(),
            size: 0,
            checksum_crc32: Default::default(),
            checksum_crc32c: Default::default(),
            checksum_sha1: Default::default(),
            checksum_sha256: Default::default(),
            checksum_crc64nvme: Default::default(),
        }
    }
}

impl ObjectPart {
    fn checksum(&self, t: &ChecksumMode) -> String {
        match t {
            ChecksumMode::ChecksumCRC32C => {
                return self.checksum_crc32c.clone();
            }
            ChecksumMode::ChecksumCRC32 => {
                return self.checksum_crc32.clone();
            }
            ChecksumMode::ChecksumSHA1 => {
                return self.checksum_sha1.clone();
            }
            ChecksumMode::ChecksumSHA256 => {
                return self.checksum_sha256.clone();
            }
            ChecksumMode::ChecksumCRC64NVME => {
                return self.checksum_crc64nvme.clone();
            }
            _ => {
                return "".to_string();
            }
        }
    }

    pub fn checksum_raw(&self, t: &ChecksumMode) -> Result<Vec<u8>, std::io::Error> {
        let b = self.checksum(t);
        if b == "" {
            return Err(std::io::Error::other("no checksum set"));
        }
        let decoded = match base64_decode(b.as_bytes()) {
            Ok(b) => b,
            Err(e) => return Err(std::io::Error::other(e)),
        };
        if decoded.len() != t.raw_byte_len() as usize {
            return Err(std::io::Error::other("checksum length mismatch"));
        }
        Ok(decoded)
    }
}

pub struct ListObjectPartsResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
    pub initiator: Initiator,
    pub owner: Owner,
    pub storage_class: String,
    pub part_number_marker: i32,
    pub next_part_number_marker: i32,
    pub max_parts: i32,
    pub checksum_algorithm: String,
    pub checksum_type: String,
    pub is_truncated: bool,
    pub object_parts: Vec<ObjectPart>,
    pub encoding_type: String,
}

#[derive(Debug, Default)]
pub struct InitiateMultipartUploadResult {
    pub bucket: String,
    pub key: String,
    pub upload_id: String,
}

#[derive(Debug, Default)]
pub struct CompleteMultipartUploadResult {
    pub location: String,
    pub bucket: String,
    pub key: String,
    pub etag: String,
    pub checksum_crc32: String,
    pub checksum_crc32c: String,
    pub checksum_sha1: String,
    pub checksum_sha256: String,
    pub checksum_crc64nvme: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct CompletePart {
    //api has
    pub etag: String,
    pub part_num: i64,
    pub checksum_crc32: String,
    pub checksum_crc32c: String,
    pub checksum_sha1: String,
    pub checksum_sha256: String,
    pub checksum_crc64nvme: String,
}

impl CompletePart {
    fn checksum(&self, t: &ChecksumMode) -> String {
        match t {
            ChecksumMode::ChecksumCRC32C => {
                return self.checksum_crc32c.clone();
            }
            ChecksumMode::ChecksumCRC32 => {
                return self.checksum_crc32.clone();
            }
            ChecksumMode::ChecksumSHA1 => {
                return self.checksum_sha1.clone();
            }
            ChecksumMode::ChecksumSHA256 => {
                return self.checksum_sha256.clone();
            }
            ChecksumMode::ChecksumCRC64NVME => {
                return self.checksum_crc64nvme.clone();
            }
            _ => {
                return "".to_string();
            }
        }
    }
}

pub struct CopyObjectPartResult {
    pub etag: String,
    pub last_modified: OffsetDateTime,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct CompleteMultipartUpload {
    pub parts: Vec<CompletePart>,
}

impl CompleteMultipartUpload {
    pub fn marshal_msg(&self) -> Result<String, std::io::Error> {
        //let buf = serde_json::to_string(self)?;
        let buf = match quick_xml::se::to_string(self) {
            Ok(buf) => buf,
            Err(e) => {
                return Err(std::io::Error::other(e));
            }
        };

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self, std::io::Error> {
        todo!();
    }
}

pub struct CreateBucketConfiguration {
    pub location: String,
}

#[derive(serde::Serialize)]
pub struct DeleteObject {
    //api has
    pub key: String,
    pub version_id: String,
}

pub struct DeletedObject {
    //s3s has
    pub key: String,
    pub version_id: String,
    pub deletemarker: bool,
    pub deletemarker_version_id: String,
}

pub struct NonDeletedObject {
    pub key: String,
    pub code: String,
    pub message: String,
    pub version_id: String,
}

#[derive(serde::Serialize)]
pub struct DeleteMultiObjects {
    pub quiet: bool,
    pub objects: Vec<DeleteObject>,
}

impl DeleteMultiObjects {
    pub fn marshal_msg(&self) -> Result<String, std::io::Error> {
        //let buf = serde_json::to_string(self)?;
        let buf = match quick_xml::se::to_string(self) {
            Ok(buf) => buf,
            Err(e) => {
                return Err(std::io::Error::other(e));
            }
        };

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self, std::io::Error> {
        todo!();
    }
}

pub struct DeleteMultiObjectsResult {
    pub deleted_objects: Vec<DeletedObject>,
    pub undeleted_objects: Vec<NonDeletedObject>,
}
