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

//! Facade modules for incremental S3 API extraction from `ecfs.rs`.
//!
//! This file intentionally starts as skeleton-only. Behavior remains in place
//! until each helper is moved with dedicated small refactor steps.

pub(crate) mod acl;
pub(crate) mod bucket;
pub(crate) mod common;
pub(crate) mod encryption {}
pub(crate) mod multipart;
/// Object helper facade placeholder.
///
/// Read-path helpers shared across storage components should live in neutral
/// modules (for example, `storage::readers`) and be consumed from there.
/// Object-specific extraction steps can be added here incrementally.
pub(crate) mod object {}
pub(crate) mod replication {}
pub(crate) mod response;
pub(crate) mod restore {}
pub(crate) mod select {}
pub(crate) mod validation {}
