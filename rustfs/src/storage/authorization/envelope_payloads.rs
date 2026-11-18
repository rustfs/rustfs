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

//! Authorization-phase payloads for request-envelope tests, which cannot reach the private
//! construction paths. Production code builds them only through the authorization paths in
//! `storage::authorization`.
//!
//! Not responsible for: any production construction. Upstream: none. Downstream: the
//! `s3_compat::envelope` tests and `storage::access::parity_test_support`.

use super::{
    BucketConfigMutationSnapshot, BucketGenerationGuard, CopySourceBucketGenerationGuard, ObjectTagConditions,
    OdmReadGenerationGuard, PendingDeleteBucketGenerationGuard, TableDataPlaneListAccess,
};
use std::collections::HashMap;

fn incarnation() -> uuid::Uuid {
    uuid::Uuid::from_u128(0x7107)
}

pub(crate) fn bucket_generation(bucket: &str) -> BucketGenerationGuard {
    BucketGenerationGuard {
        bucket: bucket.to_owned(),
        incarnation_id: incarnation(),
    }
}

pub(crate) fn pending_delete_bucket_generation(bucket: &str) -> PendingDeleteBucketGenerationGuard {
    PendingDeleteBucketGenerationGuard::Ready(bucket_generation(bucket))
}

pub(crate) fn copy_source_bucket_generation(bucket: &str) -> CopySourceBucketGenerationGuard {
    CopySourceBucketGenerationGuard {
        bucket: bucket.to_owned(),
        incarnation_id: incarnation(),
    }
}

pub(crate) fn odm_read_generation(bucket: &str) -> OdmReadGenerationGuard {
    OdmReadGenerationGuard::Ready(bucket_generation(bucket))
}

pub(crate) fn bucket_config_mutation(bucket: &str) -> BucketConfigMutationSnapshot {
    BucketConfigMutationSnapshot {
        bucket: bucket.to_owned(),
        incarnation_id: incarnation(),
    }
}

pub(crate) fn table_list_access(bucket: &str) -> TableDataPlaneListAccess {
    TableDataPlaneListAccess {
        bucket: bucket.to_owned(),
        principal_fingerprint: String::new(),
        snapshot_fingerprint: String::new(),
        cursor_key: [0; 32],
        resources: Vec::new(),
    }
}

pub(crate) fn object_tag_conditions(bucket: &str) -> ObjectTagConditions {
    ObjectTagConditions::new(bucket, "object", None, HashMap::new())
}
