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

#![allow(dead_code)]

use s3s::dto::{
    BucketKeyEnabled, BucketName, ChecksumCRC32, ChecksumCRC32C, ChecksumCRC64NVME, ChecksumSHA1, ChecksumSHA256, ChecksumType,
    ETag, Expiration, Location, ObjectKey, ObjectVersionId, RequestCharged, SSEKMSKeyId, ServerSideEncryption,
};

#[derive(Debug, Clone, Default)]
pub struct CompleteMultipartUploadOutput {
    pub bucket: Option<BucketName>,
    pub bucket_key_enabled: Option<BucketKeyEnabled>,
    pub checksum_crc32: Option<ChecksumCRC32>,
    pub checksum_crc32c: Option<ChecksumCRC32C>,
    pub checksum_crc64nvme: Option<ChecksumCRC64NVME>,
    pub checksum_sha1: Option<ChecksumSHA1>,
    pub checksum_sha256: Option<ChecksumSHA256>,
    pub checksum_type: Option<ChecksumType>,
    pub e_tag: Option<ETag>,
    pub expiration: Option<Expiration>,
    pub key: Option<ObjectKey>,
    pub location: Option<Location>,
    pub request_charged: Option<RequestCharged>,
    pub ssekms_key_id: Option<SSEKMSKeyId>,
    pub server_side_encryption: Option<ServerSideEncryption>,
    pub version_id: Option<ObjectVersionId>,
}

impl From<s3s::dto::CompleteMultipartUploadOutput> for CompleteMultipartUploadOutput {
    fn from(output: s3s::dto::CompleteMultipartUploadOutput) -> Self {
        Self {
            bucket: output.bucket,
            bucket_key_enabled: output.bucket_key_enabled,
            checksum_crc32: output.checksum_crc32,
            checksum_crc32c: output.checksum_crc32c,
            checksum_crc64nvme: output.checksum_crc64nvme,
            checksum_sha1: output.checksum_sha1,
            checksum_sha256: output.checksum_sha256,
            checksum_type: output.checksum_type,
            e_tag: output.e_tag,
            expiration: output.expiration,
            key: output.key,
            location: output.location,
            request_charged: output.request_charged,
            ssekms_key_id: output.ssekms_key_id,
            server_side_encryption: output.server_side_encryption,
            version_id: output.version_id,
        }
    }
}
