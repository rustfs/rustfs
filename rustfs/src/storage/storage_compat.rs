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
    pub(crate) use rustfs_ecstore::{
        admin_server_info, bucket, client, disk, error, get_global_lock_client, global, metrics_realtime,
        resolve_object_store_handle, rio, rpc, set_disk, store,
    };

    pub(crate) mod store_api {
        pub(crate) use rustfs_ecstore::store_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
    }

    #[cfg(test)]
    pub(crate) use rustfs_ecstore::config;
}
