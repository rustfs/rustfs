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

pub const AMZ_META_UNENCRYPTED_CONTENT_LENGTH: &str = "X-Amz-Meta-X-Amz-Unencrypted-Content-Length";
pub const AMZ_META_UNENCRYPTED_CONTENT_MD5: &str = "X-Amz-Meta-X-Amz-Unencrypted-Content-Md5";

pub const AMZ_STORAGE_CLASS: &str = "x-amz-storage-class";

pub const RESERVED_METADATA_PREFIX: &str = "X-RustFS-Internal-";
pub const RESERVED_METADATA_PREFIX_LOWER: &str = "x-rustfs-internal-";

pub const RUSTFS_HEALING: &str = "X-Rustfs-Internal-healing";
// pub const RUSTFS_DATA_MOVE: &str = "X-Rustfs-Internal-data-mov";

// pub const X_RUSTFS_INLINE_DATA: &str = "x-rustfs-inline-data";

pub const VERSION_PURGE_STATUS_KEY: &str = "X-Rustfs-Internal-purgestatus";

pub const X_RUSTFS_HEALING: &str = "X-Rustfs-Internal-healing";
pub const X_RUSTFS_DATA_MOV: &str = "X-Rustfs-Internal-data-mov";

pub const AMZ_OBJECT_TAGGING: &str = "X-Amz-Tagging";
pub const AMZ_BUCKET_REPLICATION_STATUS: &str = "X-Amz-Replication-Status";
pub const AMZ_DECODED_CONTENT_LENGTH: &str = "X-Amz-Decoded-Content-Length";

pub const RUSTFS_DATA_MOVE: &str = "X-Rustfs-Internal-data-mov";

// Server-side encryption headers
pub const AMZ_SERVER_SIDE_ENCRYPTION: &str = "x-amz-server-side-encryption";
pub const AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: &str = "x-amz-server-side-encryption-aws-kms-key-id";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT: &str = "x-amz-server-side-encryption-context";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str = "x-amz-server-side-encryption-customer-key";
pub const AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";

// SSE-C copy source headers
pub const AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: &str =
    "x-amz-copy-source-server-side-encryption-customer-algorithm";
pub const AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY: &str = "x-amz-copy-source-server-side-encryption-customer-key";
pub const AMZ_COPY_SOURCE_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: &str =
    "x-amz-copy-source-server-side-encryption-customer-key-md5";
