// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! OpenStack Swift API implementation
//!
//! This module provides support for the OpenStack Swift object storage API,
//! enabling RustFS to serve as a Swift-compatible storage backend while
//! reusing the existing S3 storage layer.
//!
//! # Architecture
//!
//! Swift requests follow the pattern: `/v1/{account}/{container}/{object}`
//! where:
//! - `account`: Tenant identifier (e.g., `AUTH_{project_id}`)
//! - `container`: Swift container (maps to S3 bucket)
//! - `object`: Object key (maps to S3 object key)
//!
//! # Authentication
//!
//! Swift API uses Keystone token-based authentication via the existing
//! `KeystoneAuthMiddleware`. The middleware validates X-Auth-Token headers
//! and stores credentials in task-local storage, which Swift handlers access
//! to enforce tenant isolation.

use std::sync::Arc;

use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::storage as ecstore_storage;

pub mod account;
pub mod acl;
pub mod bulk;
pub mod container;
pub mod cors;
pub mod dlo;
pub mod encryption;
pub mod errors;
pub mod expiration;
pub mod expiration_worker;
pub mod formpost;
pub mod handler;
pub mod object;
pub mod quota;
pub mod ratelimit;
pub mod router;
pub mod slo;
pub mod staticweb;
pub mod symlink;
pub mod sync;
pub mod tempurl;
pub mod types;
pub mod versioning;

pub use errors::{SwiftError, SwiftResult};
pub use router::{SwiftRoute, SwiftRouter};
// Note: Container, Object, and SwiftMetadata types used by Swift implementation
#[allow(unused_imports)]
pub use types::{Container, Object, SwiftMetadata};

type SwiftBucketMetadata = ecstore_bucket::metadata::BucketMetadata;
type SwiftStorageResult<T> = ecstore_error::Result<T>;
type SwiftStore = ecstore_storage::ECStore;
pub type SwiftGetObjectReader = <SwiftStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub type SwiftObjectInfo = <SwiftStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type SwiftObjectOptions = <SwiftStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type SwiftPutObjReader = <SwiftStore as rustfs_storage_api::ObjectIO>::PutObjectReader;

fn resolve_swift_object_store_handle() -> Option<Arc<SwiftStore>> {
    ecstore_global::resolve_object_store_handle()
}

async fn get_swift_bucket_metadata(bucket: &str) -> SwiftStorageResult<Arc<SwiftBucketMetadata>> {
    ecstore_bucket::metadata_sys::get(bucket).await
}

async fn set_swift_bucket_metadata(bucket: String, metadata: SwiftBucketMetadata) -> SwiftStorageResult<()> {
    ecstore_bucket::metadata_sys::set_bucket_metadata(bucket, metadata).await
}
