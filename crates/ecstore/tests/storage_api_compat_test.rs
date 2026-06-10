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

use rustfs_ecstore::store_api::{
    BucketInfo as EcstoreBucketInfo, BucketOptions as EcstoreBucketOptions, MakeBucketOptions as EcstoreMakeBucketOptions,
};
use rustfs_ecstore::{disk::DiskStore, error::Error, store::ECStore};
use rustfs_storage_api::{
    BucketInfo as ApiBucketInfo, BucketOptions as ApiBucketOptions, MakeBucketOptions as ApiMakeBucketOptions, StorageAdminApi,
};

#[test]
fn old_store_api_bucket_dto_path_reexports_storage_api_types() {
    let ecstore_bucket: EcstoreBucketInfo = ApiBucketInfo {
        name: "photos".to_owned(),
        versioning: true,
        ..Default::default()
    };
    let api_bucket: ApiBucketInfo = ecstore_bucket;

    let ecstore_make: EcstoreMakeBucketOptions = ApiMakeBucketOptions::default();
    let api_options: ApiBucketOptions = EcstoreBucketOptions::default();

    assert_eq!(api_bucket.name, "photos");
    assert!(api_bucket.versioning);
    assert!(!ecstore_make.lock_enabled);
    assert!(!api_options.no_metadata);
}

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

#[test]
fn ecstore_implements_storage_admin_api_contract() {
    assert!(storage_admin_api_type_name::<ECStore>().ends_with("::ECStore"));
}
