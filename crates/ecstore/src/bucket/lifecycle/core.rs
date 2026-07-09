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

use crate::object_api::ObjectInfo;

pub use rustfs_lifecycle::{
    Event, ExpirationOptions, IlmAction, Lifecycle, LifecycleCalculate, ObjectOpts, RuleValidate, TRANSITION_COMPLETE,
    TRANSITION_PENDING, TransitionOptions, abort_incomplete_multipart_upload_due, expected_expiry_time,
};

pub fn object_opts_from_object_info(oi: &ObjectInfo) -> ObjectOpts {
    ObjectOpts {
        name: oi.name.clone(),
        user_tags: (*oi.user_tags).clone(),
        mod_time: oi.mod_time,
        size: usize::try_from(oi.size).unwrap_or(usize::MAX),
        version_id: oi.version_id,
        is_latest: oi.is_latest,
        delete_marker: oi.delete_marker,
        num_versions: oi.num_versions,
        successor_mod_time: oi.successor_mod_time,
        transition_status: oi.transitioned_object.status.clone(),
        restore_ongoing: oi.restore_ongoing,
        restore_expires: oi.restore_expires,
        versioned: false,
        version_suspended: false,
        user_defined: (*oi.user_defined).clone(),
        version_purge_status: oi.version_purge_status.clone(),
        replication_status: oi.replication_status.clone(),
    }
}
