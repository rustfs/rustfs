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

//! Test-only ECStore compatibility boundary for the IAM integration tests.
//!
//! All direct `rustfs_ecstore` facade imports used by tests in this crate
//! must go through this module (architecture migration rule:
//! `check_architecture_migration_rules.sh`). Keep the surface minimal —
//! only what the tests actually need to build a temp-disk ECStore fixture
//! and to flip the erasure setup type for lock-quorum fault injection.

#[allow(unused_imports)]
pub(crate) mod fixture {
    pub(crate) use rustfs_ecstore::api::disk::endpoint::Endpoint;
    pub(crate) use rustfs_ecstore::api::layout::{EndpointServerPools, Endpoints, PoolEndpoints, SetupType};
    pub(crate) use rustfs_ecstore::api::storage::{ECStore, init_local_disks};

    // `update_erasure_type` is a write-side global facade entry. Its use is
    // restricted to reviewed storage_api boundaries; this test-only module is
    // registered in check_architecture_migration_rules.sh precisely so the
    // sequential-restart regression test can flip into distributed-erasure
    // mode to make namespace-lock acquisition fail (rustfs#4304).
    pub(crate) use rustfs_ecstore::api::global::update_erasure_type;
}
