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

use crate::bucket::replication::ObjectOpts;
use s3s::dto::ReplicaModificationsStatus;
use s3s::dto::ReplicationRule;

pub trait ReplicationRuleExt {
    fn prefix(&self) -> &str;
    fn metadata_replicate(&self, obj: &ObjectOpts) -> bool;
}

impl ReplicationRuleExt for ReplicationRule {
    fn prefix(&self) -> &str {
        if let Some(filter) = &self.filter {
            if let Some(prefix) = &filter.prefix {
                prefix
            } else if let Some(and) = &filter.and {
                and.prefix.as_deref().unwrap_or("")
            } else {
                ""
            }
        } else {
            ""
        }
    }

    fn metadata_replicate(&self, obj: &ObjectOpts) -> bool {
        if !obj.replica {
            return true;
        }

        self.source_selection_criteria.as_ref().is_some_and(|s| {
            s.replica_modifications
                .clone()
                .is_some_and(|r| r.status == ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED))
        })
    }
}
