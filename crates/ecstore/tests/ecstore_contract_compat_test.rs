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

use rustfs_common::heal_channel::HealOpts;
use rustfs_ecstore::{
    disk::DiskStore,
    error::Error,
    store::ECStore,
    store_api::{HealOperations, NamespaceLocking},
};
use rustfs_lock::NamespaceLockWrapper;
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_storage_api::{HealOperations as StorageHealOperations, NamespaceLocking as StorageNamespaceLocking, StorageAdminApi};

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

fn namespace_locking_type_name<T>() -> &'static str
where
    T: NamespaceLocking,
{
    std::any::type_name::<T>()
}

fn heal_operations_type_name<T>() -> &'static str
where
    T: HealOperations,
{
    std::any::type_name::<T>()
}

fn storage_namespace_locking_type_name<T>() -> &'static str
where
    T: StorageNamespaceLocking<Error = Error, NamespaceLock = NamespaceLockWrapper>,
{
    std::any::type_name::<T>()
}

fn storage_heal_operations_type_name<T>() -> &'static str
where
    T: StorageHealOperations<Error = Error, HealResultItem = HealResultItem, HealOptions = HealOpts>,
{
    std::any::type_name::<T>()
}

#[test]
fn ecstore_implements_storage_admin_api_contract() {
    assert!(storage_admin_api_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_namespace_locking_contract() {
    assert!(namespace_locking_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_heal_operations_contract() {
    assert!(heal_operations_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_namespace_locking_contract() {
    assert!(storage_namespace_locking_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_heal_operations_contract() {
    assert!(storage_heal_operations_type_name::<ECStore>().ends_with("::ECStore"));
}
