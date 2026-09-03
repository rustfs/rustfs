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

//! On-demand migration (ODM) end-to-end suite (rustfs/backlog#2147).
//!
//! `common` is the shared environment: one RustFS under test, one programmable
//! fake S3 source, admin-API wrappers, seeding and local-state assertions.
//! `harness_self_test` proves the harness itself; `get_basic_test` covers the
//! GET read-through (rustfs/backlog#2156). The fault, concurrency,
//! interaction and real-source matrix is rustfs/backlog#2158; its lane split
//! lives in `.config/nextest.toml` (fault / concurrency / real source run
//! nightly, the rest in the merge lane).

pub mod common;

mod concurrency_test;
mod fault_test;
mod get_basic_test;
mod harness_self_test;
mod interaction_test;
mod real_source_test;
