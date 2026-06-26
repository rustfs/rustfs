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

use std::sync::Arc;

pub(crate) use rustfs_ecstore::api::bucket::metadata::BucketMetadata as SwiftBucketMetadata;
use rustfs_ecstore::api::bucket::metadata_sys::{
    get as get_swift_bucket_metadata_from_backend, set_bucket_metadata as set_swift_bucket_metadata_in_backend,
};
pub(crate) use rustfs_ecstore::api::error::Result as SwiftStorageResult;
pub(crate) use rustfs_ecstore::api::global::resolve_object_store_handle as resolve_swift_object_store_handle;
use rustfs_ecstore::api::storage::ECStore as SwiftStore;
pub(crate) use rustfs_storage_api::{
    BucketInfo, BucketOperations, BucketOptions, DeleteBucketOptions, HTTPRangeSpec, ListOperations, MakeBucketOptions, ObjectIO,
    ObjectOperations,
};

pub type SwiftGetObjectReader = <SwiftStore as ObjectIO>::GetObjectReader;
pub type SwiftObjectInfo = <SwiftStore as ObjectOperations>::ObjectInfo;
pub type SwiftObjectOptions = <SwiftStore as ObjectOperations>::ObjectOptions;
pub type SwiftPutObjReader = <SwiftStore as ObjectIO>::PutObjectReader;

pub(crate) async fn get_swift_bucket_metadata(bucket: &str) -> SwiftStorageResult<Arc<SwiftBucketMetadata>> {
    get_swift_bucket_metadata_from_backend(bucket).await
}

pub(crate) async fn set_swift_bucket_metadata(bucket: String, metadata: SwiftBucketMetadata) -> SwiftStorageResult<()> {
    set_swift_bucket_metadata_in_backend(bucket, metadata).await
}
