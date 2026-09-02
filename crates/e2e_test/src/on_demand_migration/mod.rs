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
//! `harness_self_test` proves the harness itself; ODM behavior scenarios are
//! separate modules wired by later tasks.

pub mod common;

mod harness_self_test;
