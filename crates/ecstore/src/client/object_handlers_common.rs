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
use tracing::warn;

use crate::bucket::lifecycle::lifecycle;
use crate::bucket::versioning::VersioningApi;
use crate::bucket::versioning_sys::BucketVersioningSys;
use crate::store::ECStore;
use crate::store_api::{ObjectOperations, ObjectOptions, ObjectToDelete};
use rustfs_lock::MAX_DELETE_LIST;

pub async fn delete_object_versions(api: &Arc<ECStore>, bucket: &str, to_del: &[ObjectToDelete], _lc_event: lifecycle::Event) {
    let mut remaining = to_del;
    loop {
        let mut to_del = remaining;
        if to_del.len() > MAX_DELETE_LIST {
            remaining = &to_del[MAX_DELETE_LIST..];
            to_del = &to_del[..MAX_DELETE_LIST];
        } else {
            remaining = &[];
        }
        let version_suspended = match BucketVersioningSys::get(bucket).await {
            Ok(vc) => vc.suspended(),
            Err(err) => {
                warn!(bucket, error = ?err, "failed to get versioning config during lifecycle noncurrent version cleanup");
                return;
            }
        };
        let (_deleted_objs, errors) = api
            .delete_objects(
                bucket,
                to_del.to_vec(),
                ObjectOptions {
                    version_suspended,
                    ..Default::default()
                },
            )
            .await;
        for (i, err) in errors.iter().enumerate() {
            if let Some(e) = err {
                let obj_name = to_del.get(i).map(|o| o.object_name.as_str()).unwrap_or("<unknown>");
                let vid = to_del
                    .get(i)
                    .and_then(|o| o.version_id)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                warn!(bucket, object = obj_name, version_id = %vid, error = ?e, "failed to delete noncurrent version during lifecycle cleanup");
            }
        }
        if remaining.is_empty() {
            break;
        }
    }
}
