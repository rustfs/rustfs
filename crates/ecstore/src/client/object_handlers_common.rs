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

use crate::StorageAPI;
use crate::bucket::lifecycle::lifecycle;
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::store::ECStore;
use crate::store_api::{ObjectOptions, ObjectToDelete};
use rustfs_lock::MAX_DELETE_LIST;

pub async fn delete_object_versions(api: ECStore, bucket: &str, to_del: &[ObjectToDelete], _lc_event: lifecycle::Event) {
    let mut remaining = to_del;
    loop {
        let mut to_del = remaining;
        if to_del.len() > MAX_DELETE_LIST {
            remaining = &to_del[MAX_DELETE_LIST..];
            to_del = &to_del[..MAX_DELETE_LIST];
        } else {
            remaining = &[];
        }
        let vc = BucketVersioningSys::get(bucket).await.expect("err!");
        let _deleted_objs = api.delete_objects(
            bucket,
            to_del.to_vec(),
            ObjectOptions {
                //prefix_enabled_fn:  vc.prefix_enabled(""),
                version_suspended: vc.suspended(),
                ..Default::default()
            },
        );
    }
}
