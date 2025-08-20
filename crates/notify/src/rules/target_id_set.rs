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

use rustfs_targets::arn::TargetID;
use std::collections::HashSet;

/// TargetIDSet - A collection representation of TargetID.
pub type TargetIdSet = HashSet<TargetID>;

/// Provides a Go-like method for TargetIdSet (can be implemented as trait if needed)
#[allow(dead_code)]
pub(crate) fn new_target_id_set(target_ids: Vec<TargetID>) -> TargetIdSet {
    target_ids.into_iter().collect()
}

// HashSet has built-in clone, union, difference and other operations.
// But the Go version of the method returns a new Set, and the HashSet method is usually iterator or modify itself.
// If you need to exactly match Go's API style, you can add wrapper functions.
