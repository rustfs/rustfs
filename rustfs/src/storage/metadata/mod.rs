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

//! Metadata engine for RustFS.
//!
//! This module implements the "Dual Metadata Center" architecture, providing
//! high-performance metadata management using SurrealKV, Ferntree, and SurrealMX.

pub mod engine;
pub mod ferntree;
pub mod kv;
pub mod mx;
pub(crate) mod reader;
pub mod types;

pub use engine::LocalMetadataEngine;

use std::sync::{Arc, OnceLock};

/// Environment variable name for enabling the new metadata engine.
/// This allows users to switch between the old and new metadata engines without code changes,
/// simply by setting this environment variable to `true` or `false`.
const ENV_NEW_METADATA_ENGINE: &str = "RUSTFS_NEW_METADATA_ENGINE";

/// Default value for enabling the new metadata engine.
/// This can be overridden by setting the environment variable `RUSTFS_NEW_METADATA_ENGINE` to `true`.
/// Setting this to `true` will enable the new metadata engine by default,
/// while `false` will keep using the old engine until explicitly enabled.
const DEFAULT_NEW_METADATA_ENGINE: bool = false;

pub static GLOBAL_METADATA_ENGINE: OnceLock<Arc<LocalMetadataEngine>> = OnceLock::new();

pub fn get_metadata_engine() -> Option<Arc<LocalMetadataEngine>> {
    GLOBAL_METADATA_ENGINE.get().cloned()
}

/// Checks if the new metadata engine is enabled via environment variable.
/// This allows for dynamic switching between the old and new metadata engines without code changes.
pub fn is_new_metadata_engine_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_NEW_METADATA_ENGINE, DEFAULT_NEW_METADATA_ENGINE)
}
