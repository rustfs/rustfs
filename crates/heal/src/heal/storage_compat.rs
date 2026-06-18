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

pub(crate) mod ecstore {
    pub(crate) use rustfs_ecstore::{data_usage, disk, error, global, store};
}
pub(crate) use self::ecstore::{
    data_usage::DATA_USAGE_CACHE_NAME,
    disk::{BUCKET_META_PREFIX, DiskAPI, DiskStore, RUSTFS_META_BUCKET, endpoint::Endpoint, error::DiskError},
    error::{Error as EcstoreError, StorageError},
    global::GLOBAL_LOCAL_DISK_MAP,
    store::ECStore,
};

#[cfg(test)]
pub(crate) use self::ecstore::disk::{DiskOption, new_disk};

pub type HealObjectInfo = rustfs_ecstore::store_api::ObjectInfo;
pub type HealObjectOptions = rustfs_ecstore::store_api::ObjectOptions;
pub type HealPutObjReader = rustfs_ecstore::store_api::PutObjReader;
