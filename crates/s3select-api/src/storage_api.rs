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

pub(crate) use rustfs_ecstore::api::error::StorageError as SelectStorageError;
use rustfs_ecstore::api::error::{
    is_err_bucket_not_found as select_is_err_bucket_not_found_from_backend,
    is_err_object_not_found as select_is_err_object_not_found_from_backend,
    is_err_version_not_found as select_is_err_version_not_found_from_backend,
};
use rustfs_ecstore::api::global::resolve_object_store_handle as resolve_select_object_store_handle_from_backend;
pub(crate) use rustfs_ecstore::api::set_disk::DEFAULT_READ_BUFFER_SIZE as SELECT_DEFAULT_READ_BUFFER_SIZE;
pub(crate) use rustfs_ecstore::api::storage::ECStore as SelectStore;
pub(crate) use rustfs_storage_api::{HTTPRangeSpec, ObjectIO, ObjectOperations};

pub(crate) type SelectGetObjectReader = <SelectStore as rustfs_storage_api::ObjectIO>::GetObjectReader;
pub(crate) type SelectObjectInfo = <SelectStore as rustfs_storage_api::ObjectOperations>::ObjectInfo;
pub(crate) type SelectObjectOptions = <SelectStore as rustfs_storage_api::ObjectOperations>::ObjectOptions;

pub(crate) fn resolve_select_object_store_handle() -> Option<Arc<SelectStore>> {
    resolve_select_object_store_handle_from_backend()
}

pub(crate) fn select_is_err_bucket_not_found(err: &SelectStorageError) -> bool {
    select_is_err_bucket_not_found_from_backend(err)
}

pub(crate) fn select_is_err_object_not_found(err: &SelectStorageError) -> bool {
    select_is_err_object_not_found_from_backend(err)
}

pub(crate) fn select_is_err_version_not_found(err: &SelectStorageError) -> bool {
    select_is_err_version_not_found_from_backend(err)
}
