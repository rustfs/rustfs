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

use convert_case::{Case, Casing};
use std::collections::HashMap;
use std::sync::LazyLock;

pub const LAST_MODIFIED: &str = "Last-Modified";
pub const DATE: &str = "Date";
pub const ETAG: &str = "ETag";
pub const CONTENT_TYPE: &str = "Content-Type";
pub const CONTENT_MD5: &str = "Content-Md5";
pub const CONTENT_ENCODING: &str = "Content-Encoding";
pub const EXPIRES: &str = "Expires";
pub const CONTENT_LENGTH: &str = "Content-Length";
pub const CONTENT_LANGUAGE: &str = "Content-Language";
pub const CONTENT_RANGE: &str = "Content-Range";
pub const CONNECTION: &str = "Connection";
pub const ACCEPT_RANGES: &str = "Accept-Ranges";
pub const AMZ_BUCKET_REGION: &str = "X-Amz-Bucket-Region";
pub const SERVER_INFO: &str = "Server";
pub const RETRY_AFTER: &str = "Retry-After";
pub const LOCATION: &str = "Location";
pub const CACHE_CONTROL: &str = "Cache-Control";
pub const CONTENT_DISPOSITION: &str = "Content-Disposition";
pub const AUTHORIZATION: &str = "Authorization";
pub const ACTION: &str = "Action";
pub const RANGE: &str = "Range";

// S3 storage class
pub const AMZ_STORAGE_CLASS: &str = "x-amz-storage-class";

// S3 object version ID
pub const AMZ_VERSION_ID: &str = "x-amz-version-id";
pub const AMZ_DELETE_MARKER: &str = "x-amz-delete-marker";

// S3 object tagging
pub const AMZ_OBJECT_TAGGING: &str = "X-Amz-Tagging";
pub const AMZ_TAG_COUNT: &str = "x-amz-tagging-count";
pub const AMZ_TAG_DIRECTIVE: &str = "X-Amz-Tagging-Directive";

// S3 transition restore
pub const AMZ_RESTORE: &str = "x-amz-restore";
pub const AMZ_RESTORE_EXPIRY_DAYS: &str = "X-Amz-Restore-Expiry-Days";
pub const AMZ_RESTORE_REQUEST_DATE: &str = "X-Amz-Restore-Request-Date";

// S3 extensions
pub const AMZ_COPY_SOURCE_IF_MODIFIED_SINCE: &str = "x-amz-copy-source-if-modified-since";
pub const AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE: &str = "x-amz-copy-source-if-unmodified-since";

pub const AMZ_COPY_SOURCE_IF_NONE_MATCH: &str = "x-amz-copy-source-if-none-match";
pub const AMZ_COPY_SOURCE_IF_MATCH: &str = "x-amz-copy-source-if-match";

pub const AMZ_COPY_SOURCE: &str = "X-Amz-Copy-Source";
pub const AMZ_COPY_SOURCE_VERSION_ID: &str = "X-Amz-Copy-Source-Version-Id";
pub const AMZ_COPY_SOURCE_RANGE: &str = "X-Amz-Copy-Source-Range";
pub const AMZ_METADATA_DIRECTIVE: &str = "X-Amz-Metadata-Directive";
pub const AMZ_OBJECT_LOCK_MODE: &str = "X-Amz-Object-Lock-Mode";
pub const AMZ_OBJECT_LOCK_MODE_LOWER: &str = "x-amz-object-lock-mode";
pub const AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE: &str = "X-Amz-Object-Lock-Retain-Until-Date";
pub const AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER: &str = "x-amz-object-lock-retain-until-date";
pub const AMZ_OBJECT_LOCK_LEGAL_HOLD: &str = "X-Amz-Object-Lock-Legal-Hold";
pub const AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER: &str = "x-amz-object-lock-legal-hold";
pub const AMZ_OBJECT_LOCK_BYPASS_GOVERNANCE: &str = "X-Amz-Bypass-Governance-Retention";
pub const AMZ_BUCKET_REPLICATION_STATUS: &str = "X-Amz-Replication-Status";

// AmzSnowballExtract will trigger unpacking of an archive content
pub const AMZ_SNOWBALL_EXTRACT: &str = "X-Amz-Meta-Snowball-Auto-Extract";

// Object lock enabled
pub const AMZ_OBJECT_LOCK_ENABLED: &str = "x-amz-bucket-object-lock-enabled";

// Multipart parts count
pub const AMZ_MP_PARTS_COUNT: &str = "x-amz-mp-parts-count";

// Object date/time of expiration
pub const AMZ_EXPIRATION: &str = "x-amz-expiration";

// Dummy putBucketACL
pub const AMZ_ACL: &str = "x-amz-acl";

// Signature V4 related constants.
pub const AMZ_CONTENT_SHA256: &str = "X-Amz-Content-Sha256";
pub const AMZ_DATE: &str = "X-Amz-Date";
pub const AMZ_ALGORITHM: &str = "X-Amz-Algorithm";
pub const AMZ_EXPIRES: &str = "X-Amz-Expires";
pub const AMZ_SIGNED_HEADERS: &str = "X-Amz-SignedHeaders";
pub const AMZ_SIGNATURE: &str = "X-Amz-Signature";
pub const AMZ_CREDENTIAL: &str = "X-Amz-Credential";
pub const AMZ_SECURITY_TOKEN: &str = "X-Amz-Security-Token";
pub const AMZ_DECODED_CONTENT_LENGTH: &str = "X-Amz-Decoded-Content-Length";
pub const AMZ_TRAILER: &str = "X-Amz-Trailer";
pub const AMZ_MAX_PARTS: &str = "X-Amz-Max-Parts";
pub const AMZ_PART_NUMBER_MARKER: &str = "X-Amz-Part-Number-Marker";

// Constants used for GetObjectAttributes and GetObjectVersionAttributes
pub const AMZ_OBJECT_ATTRIBUTES: &str = "X-Amz-Object-Attributes";

// AWS server-side encryption headers for SSE-S3, SSE-KMS and SSE-C.
pub const AMZ_SERVER_SIDE_ENCRYPTION: &str = "X-Amz-Server-Side-Encryption";
pub const AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID: &str = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id";
pub const AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT: &str = "X-Amz-Server-Side-Encryption-Context";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str = "X-Amz-Server-Side-Encryption-Customer-Algorithm";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str = "X-Amz-Server-Side-Encryption-Customer-Key";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str = "X-Amz-Server-Side-Encryption-Customer-Key-Md5";
pub const AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_ALGORITHM: &str =
    "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm";
pub const AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY: &str = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key";
pub const AMZ_SERVER_SIDE_ENCRYPTION_COPY_CUSTOMER_KEY_MD5: &str = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-Md5";

pub const AMZ_ENCRYPTION_AES: &str = "AES256";
pub const AMZ_ENCRYPTION_KMS: &str = "aws:kms";

// Signature v2 related constants
pub const AMZ_SIGNATURE_V2: &str = "Signature";
pub const AMZ_ACCESS_KEY_ID: &str = "AWSAccessKeyId";

// Response request id.
pub const AMZ_REQUEST_ID: &str = "x-amz-request-id";
pub const AMZ_REQUEST_HOST_ID: &str = "x-amz-id-2";

// Content Checksums
pub const AMZ_CHECKSUM_ALGO: &str = "x-amz-checksum-algorithm";
pub const AMZ_CHECKSUM_CRC32: &str = "x-amz-checksum-crc32";
pub const AMZ_CHECKSUM_CRC32C: &str = "x-amz-checksum-crc32c";
pub const AMZ_CHECKSUM_SHA1: &str = "x-amz-checksum-sha1";
pub const AMZ_CHECKSUM_SHA256: &str = "x-amz-checksum-sha256";
pub const AMZ_CHECKSUM_CRC64NVME: &str = "x-amz-checksum-crc64nvme";
pub const AMZ_CHECKSUM_MODE: &str = "x-amz-checksum-mode";
pub const AMZ_CHECKSUM_TYPE: &str = "x-amz-checksum-type";
pub const AMZ_CHECKSUM_TYPE_FULL_OBJECT: &str = "FULL_OBJECT";
pub const AMZ_CHECKSUM_TYPE_COMPOSITE: &str = "COMPOSITE";

// Post Policy related
pub const AMZ_META_UUID: &str = "X-Amz-Meta-Uuid";
pub const AMZ_META_NAME: &str = "X-Amz-Meta-Name";

pub const AMZ_META_UNENCRYPTED_CONTENT_LENGTH: &str = "X-Amz-Meta-X-Amz-Unencrypted-Content-Length";
pub const AMZ_META_UNENCRYPTED_CONTENT_MD5: &str = "X-Amz-Meta-X-Amz-Unencrypted-Content-Md5";

pub const RESERVED_METADATA_PREFIX: &str = "X-RustFS-Internal-";
pub const RESERVED_METADATA_PREFIX_LOWER: &str = "x-rustfs-internal-";

pub const RUSTFS_HEALING: &str = "X-Rustfs-Internal-healing";
// pub const RUSTFS_DATA_MOVE: &str = "X-Rustfs-Internal-data-mov";

// pub const X_RUSTFS_INLINE_DATA: &str = "x-rustfs-inline-data";

pub const VERSION_PURGE_STATUS_KEY: &str = "X-Rustfs-Internal-purgestatus";

pub const X_RUSTFS_HEALING: &str = "X-Rustfs-Internal-healing";
pub const X_RUSTFS_DATA_MOV: &str = "X-Rustfs-Internal-data-mov";

pub const AMZ_TAGGING_DIRECTIVE: &str = "X-Amz-Tagging-Directive";

pub const RUSTFS_DATA_MOVE: &str = "X-Rustfs-Internal-data-mov";

pub const RUSTFS_FORCE_DELETE: &str = "X-Rustfs-Force-Delete";
pub const RUSTFS_INCLUDE_DELETED: &str = "X-Rustfs-Include-Deleted";

pub const RUSTFS_REPLICATION_RESET_STATUS: &str = "X-Rustfs-Replication-Reset-Status";
pub const RUSTFS_REPLICATION_ACTUAL_OBJECT_SIZE: &str = "X-Rustfs-Replication-Actual-Object-Size";

pub const RUSTFS_BUCKET_SOURCE_VERSION_ID: &str = "X-Rustfs-Source-Version-Id";
pub const RUSTFS_BUCKET_SOURCE_MTIME: &str = "X-Rustfs-Source-Mtime";
pub const RUSTFS_BUCKET_SOURCE_ETAG: &str = "X-Rustfs-Source-Etag";
pub const RUSTFS_BUCKET_REPLICATION_DELETE_MARKER: &str = "X-Rustfs-Source-DeleteMarker";
pub const RUSTFS_BUCKET_REPLICATION_PROXY_REQUEST: &str = "X-Rustfs-Source-Proxy-Request";
pub const RUSTFS_BUCKET_REPLICATION_REQUEST: &str = "X-Rustfs-Source-Replication-Request";
pub const RUSTFS_BUCKET_REPLICATION_CHECK: &str = "X-Rustfs-Source-Replication-Check";
pub const RUSTFS_BUCKET_REPLICATION_SSEC_CHECKSUM: &str = "X-Rustfs-Source-Replication-Ssec-Crc";

// SSEC encryption header constants
pub const SSEC_ALGORITHM_HEADER: &str = "x-amz-server-side-encryption-customer-algorithm";
pub const SSEC_KEY_HEADER: &str = "x-amz-server-side-encryption-customer-key";
pub const SSEC_KEY_MD5_HEADER: &str = "x-amz-server-side-encryption-customer-key-md5";

pub const AMZ_WEBSITE_REDIRECT_LOCATION: &str = "x-amz-website-redirect-location";

pub trait HeaderExt {
    fn lookup(&self, s: &str) -> Option<&str>;
}

impl HeaderExt for HashMap<String, String> {
    fn lookup(&self, s: &str) -> Option<&str> {
        let train = s.to_case(Case::Train);
        let lower = s.to_ascii_lowercase();
        let keys = [s, lower.as_str(), train.as_str()];

        for key in keys {
            if let Some(v) = self.get(key) {
                return Some(v);
            }
        }

        None
    }
}

static SUPPORTED_QUERY_VALUES: LazyLock<HashMap<String, bool>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("attributes".to_string(), true);
    m.insert("partNumber".to_string(), true);
    m.insert("versionId".to_string(), true);
    m.insert("response-cache-control".to_string(), true);
    m.insert("response-content-disposition".to_string(), true);
    m.insert("response-content-encoding".to_string(), true);
    m.insert("response-content-language".to_string(), true);
    m.insert("response-content-type".to_string(), true);
    m.insert("response-expires".to_string(), true);
    m
});
static SUPPORTED_HEADERS: LazyLock<HashMap<String, bool>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("content-type".to_string(), true);
    m.insert("cache-control".to_string(), true);
    m.insert("content-encoding".to_string(), true);
    m.insert("content-disposition".to_string(), true);
    m.insert("content-language".to_string(), true);
    m.insert("x-amz-website-redirect-location".to_string(), true);
    m.insert("x-amz-object-lock-mode".to_string(), true);
    m.insert("x-amz-metadata-directive".to_string(), true);
    m.insert("x-amz-object-lock-retain-until-date".to_string(), true);
    m.insert("expires".to_string(), true);
    m.insert("x-amz-replication-status".to_string(), true);
    m
});
static SSE_HEADERS: LazyLock<HashMap<String, bool>> = LazyLock::new(|| {
    let mut m = HashMap::new();
    m.insert("x-amz-server-side-encryption".to_string(), true);
    m.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), true);
    m.insert("x-amz-server-side-encryption-context".to_string(), true);
    m.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), true);
    m.insert("x-amz-server-side-encryption-customer-key".to_string(), true);
    m.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), true);
    m
});

pub fn is_standard_query_value(qs_key: &str) -> bool {
    *SUPPORTED_QUERY_VALUES.get(qs_key).unwrap_or(&false)
}

pub fn is_storageclass_header(header_key: &str) -> bool {
    header_key.to_lowercase() == AMZ_STORAGE_CLASS.to_lowercase()
}

pub fn is_standard_header(header_key: &str) -> bool {
    *SUPPORTED_HEADERS.get(&header_key.to_lowercase()).unwrap_or(&false)
}

pub fn is_sse_header(header_key: &str) -> bool {
    *SSE_HEADERS.get(&header_key.to_lowercase()).unwrap_or(&false)
}

pub fn is_amz_header(header_key: &str) -> bool {
    let key = header_key.to_lowercase();
    key.starts_with("x-amz-meta-")
        || key.starts_with("x-amz-grant-")
        || key == "x-amz-acl"
        || is_sse_header(header_key)
        || key.starts_with("x-amz-checksum-")
}

pub fn is_rustfs_header(header_key: &str) -> bool {
    header_key.to_lowercase().starts_with("x-rustfs-")
}

pub fn is_minio_header(header_key: &str) -> bool {
    header_key.to_lowercase().starts_with("x-minio-")
}
