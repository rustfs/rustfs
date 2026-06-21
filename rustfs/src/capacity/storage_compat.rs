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

use rustfs_ecstore::api::{disk as ecstore_disk, storage as ecstore_storage};

pub(crate) type Endpoint = ecstore_disk::endpoint::Endpoint;

pub(crate) trait CapacityDiskExt {
    fn endpoint(&self) -> Endpoint;
    fn to_string(&self) -> String;
}

impl<T> CapacityDiskExt for T
where
    T: ecstore_disk::DiskAPI,
{
    fn endpoint(&self) -> Endpoint {
        ecstore_disk::DiskAPI::endpoint(self)
    }

    fn to_string(&self) -> String {
        ecstore_disk::DiskAPI::to_string(self)
    }
}

pub(crate) async fn all_local_disk() -> Vec<ecstore_disk::DiskStore> {
    ecstore_storage::all_local_disk().await
}
