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
use rustfs_ecstore::api::object::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use rustfs_ecstore::api::{disk::DiskStore, error::Error, storage::ECStore};
use rustfs_filemeta::FileInfo;
use rustfs_lock::NamespaceLockWrapper;
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_storage_api::{
    CompletePart, DeletedObject, HTTPRangeSpec, HealOperations as StorageHealOperations, ListMultipartsInfo,
    ListObjectVersionsInfo as StorageListObjectVersionsInfo, ListObjectsV2Info as StorageListObjectsV2Info, ListPartsInfo,
    MultipartInfo, MultipartOperations as StorageMultipartOperations, MultipartUploadResult,
    NamespaceLocking as StorageNamespaceLocking, ObjectIO as StorageObjectIO, ObjectInfoOrErr as StorageObjectInfoOrErr,
    ObjectOperations as StorageObjectOperations, ObjectToDelete, PartInfo, StorageAdminApi, WalkOptions as StorageWalkOptions,
};
use tokio_util::sync::CancellationToken;

type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
type WalkOptions = StorageWalkOptions<fn(&FileInfo) -> bool>;

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

fn storage_namespace_locking_type_name<T>() -> &'static str
where
    T: StorageNamespaceLocking<Error = Error, NamespaceLock = NamespaceLockWrapper>,
{
    std::any::type_name::<T>()
}

fn storage_object_io_type_name<T>() -> &'static str
where
    T: StorageObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = http::HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        >,
{
    std::any::type_name::<T>()
}

fn storage_object_operations_type_name<T>() -> &'static str
where
    T: StorageObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    std::any::type_name::<T>()
}

fn storage_list_operations_type_name<T>() -> &'static str
where
    T: rustfs_storage_api::ListOperations<
            Error = Error,
            ListObjectsV2Info = ListObjectsV2Info,
            ListObjectVersionsInfo = ListObjectVersionsInfo,
            ObjectInfoOrErr = ObjectInfoOrErr,
            WalkOptions = WalkOptions,
            WalkCancellation = CancellationToken,
            WalkResultSender = tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        >,
{
    std::any::type_name::<T>()
}

fn storage_multipart_operations_type_name<T>() -> &'static str
where
    T: StorageMultipartOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            PutObjectReader = PutObjReader,
            CompletePart = CompletePart,
            ListMultipartsInfo = ListMultipartsInfo,
            MultipartUploadResult = MultipartUploadResult,
            PartInfo = PartInfo,
            MultipartInfo = MultipartInfo,
            ListPartsInfo = ListPartsInfo,
        >,
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
fn ecstore_implements_storage_namespace_locking_contract() {
    assert!(storage_namespace_locking_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_object_io_contract() {
    assert!(storage_object_io_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_object_operations_contract() {
    assert!(storage_object_operations_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_list_operations_contract() {
    assert!(storage_list_operations_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_multipart_operations_contract() {
    assert!(storage_multipart_operations_type_name::<ECStore>().ends_with("::ECStore"));
}

#[test]
fn ecstore_implements_storage_heal_operations_contract() {
    assert!(storage_heal_operations_type_name::<ECStore>().ends_with("::ECStore"));
}
