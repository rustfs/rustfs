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

use std::sync::Arc;

use rustfs_ecstore::api::bucket as ecstore_bucket;
use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::storage as ecstore_storage;

pub(super) type SwiftBucketMetadata = ecstore_bucket::metadata::BucketMetadata;
pub(super) type SwiftStorageResult<T> = ecstore_error::Result<T>;
pub(super) type SwiftStore = ecstore_storage::ECStore;
pub type SwiftGetObjectReader = <SwiftStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub type SwiftObjectInfo = <SwiftStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub type SwiftObjectOptions = <SwiftStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;
pub type SwiftPutObjReader = <SwiftStore as rustfs_storage_api::ObjectIO>::PutObjectReader;

pub fn resolve_swift_object_store_handle() -> Option<Arc<SwiftStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub async fn get_swift_bucket_metadata(bucket: &str) -> SwiftStorageResult<Arc<SwiftBucketMetadata>> {
    ecstore_bucket::metadata_sys::get(bucket).await
}

pub async fn set_swift_bucket_metadata(bucket: String, metadata: SwiftBucketMetadata) -> SwiftStorageResult<()> {
    ecstore_bucket::metadata_sys::set_bucket_metadata(bucket, metadata).await
}
