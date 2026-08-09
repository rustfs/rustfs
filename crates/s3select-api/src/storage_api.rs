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
#[cfg(test)]
pub(crate) use rustfs_ecstore::api::object::PutObjReader as SelectPutObjReader;
pub use rustfs_ecstore::api::object::SelectObjectSnapshot;
pub(crate) use rustfs_ecstore::api::object::{
    PrepareSelectObjectSnapshotError, SelectObjectSnapshotReadError, SnapshotConsistencyError,
};
use rustfs_ecstore::api::runtime::object_store_handle as resolve_select_object_store_handle_from_backend;
pub(crate) use rustfs_ecstore::api::set_disk::DEFAULT_READ_BUFFER_SIZE as SELECT_DEFAULT_READ_BUFFER_SIZE;
pub(crate) use rustfs_ecstore::api::storage::ECStore as SelectStore;
use rustfs_storage_api as storage_contracts;

#[cfg(test)]
static SELECT_TEST_OBJECT_STORE: std::sync::OnceLock<Arc<SelectStore>> = std::sync::OnceLock::new();

pub(crate) mod object_store {
    pub(crate) use super::storage_contracts::HTTPRangeSpec;
    #[cfg(test)]
    pub(crate) use super::storage_contracts::ObjectIO;
}

pub(crate) mod crate_boundary {
    pub(crate) use super::{
        PrepareSelectObjectSnapshotError, SELECT_DEFAULT_READ_BUFFER_SIZE, SelectGetObjectReader, SelectObjectOptions,
        SelectObjectSnapshotReadError, SelectStorageError, SelectStore, SnapshotConsistencyError,
        resolve_select_object_store_handle, select_is_err_bucket_not_found, select_is_err_object_not_found,
        select_is_err_version_not_found,
    };
}

pub(crate) type SelectGetObjectReader = <SelectStore as storage_contracts::ObjectIO>::GetObjectReader;
pub(crate) type SelectObjectOptions = <SelectStore as storage_contracts::ObjectOperations>::ObjectOptions;

#[cfg(test)]
pub(crate) async fn select_test_ecstore_env() -> &'static rustfs_test_utils::TestECStoreEnv {
    static ENV: tokio::sync::OnceCell<rustfs_test_utils::TestECStoreEnv> = tokio::sync::OnceCell::const_new();
    let env = ENV
        .get_or_init(|| async { rustfs_test_utils::TestECStoreEnv::builder().build().await })
        .await;
    let _ = SELECT_TEST_OBJECT_STORE.set(Arc::clone(&env.ecstore));
    env
}

pub(crate) fn resolve_select_object_store_handle() -> Option<Arc<SelectStore>> {
    #[cfg(test)]
    if let Some(store) = SELECT_TEST_OBJECT_STORE.get() {
        return Some(Arc::clone(store));
    }
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
