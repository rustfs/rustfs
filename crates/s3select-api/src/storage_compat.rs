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

use rustfs_ecstore::api::error as ecstore_error;
use rustfs_ecstore::api::global as ecstore_global;
use rustfs_ecstore::api::set_disk as ecstore_set_disk;
use rustfs_ecstore::api::storage as ecstore_storage;

pub(crate) type SelectStorageError = ecstore_error::StorageError;
pub(crate) type SelectStore = ecstore_storage::ECStore;
pub(crate) type SelectGetObjectReader = <SelectStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub(crate) type SelectObjectInfo = <SelectStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub(crate) type SelectObjectOptions = <SelectStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;

pub(crate) const SELECT_DEFAULT_READ_BUFFER_SIZE: usize = ecstore_set_disk::DEFAULT_READ_BUFFER_SIZE;

pub(crate) fn select_default_read_buffer_size_u64() -> u64 {
    u64::try_from(SELECT_DEFAULT_READ_BUFFER_SIZE).unwrap_or(u64::MAX)
}

pub(crate) fn resolve_select_object_store_handle() -> Option<std::sync::Arc<SelectStore>> {
    ecstore_global::resolve_object_store_handle()
}

pub(crate) fn select_is_err_bucket_not_found(err: &SelectStorageError) -> bool {
    ecstore_error::is_err_bucket_not_found(err)
}

pub(crate) fn select_is_err_object_not_found(err: &SelectStorageError) -> bool {
    ecstore_error::is_err_object_not_found(err)
}

pub(crate) fn select_is_err_version_not_found(err: &SelectStorageError) -> bool {
    ecstore_error::is_err_version_not_found(err)
}
