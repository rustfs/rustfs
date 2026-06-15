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

use rustfs_ecstore::{
    disk::DiskStore,
    error::Error,
    store::ECStore,
    store_api::{NamespaceLocking, StorageAPI},
};
use rustfs_storage_api::StorageAdminApi;

fn storage_admin_api_type_name<T>() -> &'static str
where
    T: StorageAdminApi<
            BackendInfo = rustfs_madmin::BackendInfo,
            StorageInfo = rustfs_madmin::StorageInfo,
            Disk = DiskStore,
            Error = Error,
        >,
{
    std::any::type_name::<T>()
}

fn storage_api_with_namespace_locking_type_name<T>() -> &'static str
where
    T: StorageAPI + NamespaceLocking,
{
    std::any::type_name::<T>()
}

#[test]
fn ecstore_implements_storage_admin_api_contract() {
    assert!(storage_admin_api_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_api_and_namespace_locking_contracts() {
    assert!(storage_api_with_namespace_locking_type_name::<ECStore>().ends_with("::ECStore"));
}
