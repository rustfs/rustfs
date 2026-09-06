// Copyright 2026 RustFS Team
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

//! 4-node 4-drive distributed e2e coverage.
//!
//! Selected by `[profile.e2e-distributed]` and run from
//! `.github/workflows/e2e-distributed.yml`. Excluded from `e2e-full` because
//! each case starts four real `rustfs` processes.

mod chaos_test;
mod concurrency_stability_test;
mod concurrent_data_movement_test;
mod data_integrity_movement_test;
mod expand_decommission_rebalance_test;
mod extra_test;
mod harness;
mod object_lock_test;
mod observability_test;
mod replication_quota_test;
mod s3_basic_test;
mod s3_during_data_movement_test;
mod site_replication_test;
mod upgrade_test;
mod versioning_test;
