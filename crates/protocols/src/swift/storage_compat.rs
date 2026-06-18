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

mod ecstore {
    pub(super) use rustfs_ecstore::{bucket, error, resolve_object_store_handle, store};
}
use self::ecstore::{
    bucket::{metadata::BucketMetadata, metadata_sys},
    error::Result as EcstoreResult,
    store::ECStore,
};
use std::sync::Arc;

pub type SwiftGetObjectReader = rustfs_ecstore::store_api::GetObjectReader;
pub type SwiftObjectInfo = rustfs_ecstore::store_api::ObjectInfo;
pub type SwiftObjectOptions = rustfs_ecstore::store_api::ObjectOptions;
pub type SwiftPutObjReader = rustfs_ecstore::store_api::PutObjReader;

pub fn resolve_swift_object_store_handle() -> Option<Arc<ECStore>> {
    ecstore::resolve_object_store_handle()
}

pub async fn get_swift_bucket_metadata(bucket: &str) -> EcstoreResult<Arc<BucketMetadata>> {
    metadata_sys::get(bucket).await
}

pub async fn set_swift_bucket_metadata(bucket: String, metadata: BucketMetadata) -> EcstoreResult<()> {
    metadata_sys::set_bucket_metadata(bucket, metadata).await
}
