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
mod error;
pub mod ferntree;
pub(crate) mod gc;
pub mod kv;
pub mod mx;
pub(crate) mod reader;
pub mod types;
pub(crate) mod writer;

use std::sync::{Arc, OnceLock};

pub use engine::LocalMetadataEngine;

/// Environment variable name for enabling the new metadata engine.
/// This allows users to switch between the old and new metadata engines without code changes,
/// simply by setting this environment variable to `true` or `false`.
const ENV_NEW_METADATA_ENGINE: &str = "RUSTFS_NEW_METADATA_ENGINE";

/// Default value for enabling the new metadata engine.
/// This can be overridden by setting the environment variable `RUSTFS_NEW_METADATA_ENGINE` to `true`.
/// Setting this to `true` will enable the new metadata engine by default,
/// while `false` will keep using the old engine until explicitly enabled.
const DEFAULT_NEW_METADATA_ENGINE: bool = false;

pub static GLOBAL_METADATA_ENGINE: OnceLock<Arc<dyn engine::MetadataEngine>> = OnceLock::new();

pub fn get_metadata_engine() -> Option<Arc<dyn engine::MetadataEngine>> {
    GLOBAL_METADATA_ENGINE.get().cloned()
}

/// Checks if the new metadata engine is enabled via environment variable.
/// This allows for dynamic switching between the old and new metadata engines without code changes.
pub fn is_new_metadata_engine_enabled() -> bool {
    rustfs_utils::get_env_bool(ENV_NEW_METADATA_ENGINE, DEFAULT_NEW_METADATA_ENGINE)
}

/// Initialise the global metadata engine if enabled via environment variable.
///
/// This is intended to be called once during server startup.  It discovers the
/// first local disk from `GLOBAL_LOCAL_DISK_MAP`, creates the KV/Ferntree/MX
/// sub-systems under `<disk_root>/.rustfs.sys/metadata/`, and spawns the
/// background GC task.
pub async fn init_metadata_engine() {
    use crate::storage::metadata::ferntree::new_index_tree;
    use crate::storage::metadata::gc::GarbageCollector;
    use crate::storage::metadata::kv::new_kv_store;
    use crate::storage::metadata::mx::MxStorageManager;
    use rustfs_ecstore::disk::DiskAPI;
    use surrealkv::Tree as KvStore;

    if !is_new_metadata_engine_enabled() {
        return;
    }

    // Locate the first local disk.
    let disk_map = rustfs_ecstore::global::GLOBAL_LOCAL_DISK_MAP.read().await;
    let mut legacy_fs = None;
    for disk in disk_map.values().flatten() {
        if let rustfs_ecstore::disk::Disk::Local(wrapper) = disk.as_ref() {
            legacy_fs = Some(wrapper.get_disk());
            break;
        }
    }
    drop(disk_map);

    let legacy_fs = match legacy_fs {
        Some(d) => d,
        None => {
            tracing::warn!(target: "rustfs::metadata", "No local disk found; skipping metadata engine init.");
            return;
        }
    };

    let root_path = legacy_fs.path();
    let meta_path = root_path.join(".rustfs.sys/metadata");

    if let Err(e) = tokio::fs::create_dir_all(&meta_path).await {
        tracing::error!(target: "rustfs::metadata", "Failed to create metadata directory: {}", e);
        return;
    }

    let kv_store: Arc<KvStore> = match new_kv_store(&meta_path.join("kv")).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::error!(target: "rustfs::metadata", "Failed to init KV store: {}", e);
            return;
        }
    };

    let index_tree = match new_index_tree().await {
        Ok(t) => Arc::new(t),
        Err(e) => {
            tracing::error!(target: "rustfs::metadata", "Failed to init index tree: {}", e);
            return;
        }
    };

    let storage_manager = match MxStorageManager::new(meta_path.join("mx")) {
        Ok(m) => Arc::new(m),
        Err(e) => {
            tracing::error!(target: "rustfs::metadata", "Failed to init storage manager: {}", e);
            return;
        }
    };

    let engine = Arc::new(LocalMetadataEngine::new(kv_store.clone(), index_tree, storage_manager.clone(), legacy_fs));

    let _ = GLOBAL_METADATA_ENGINE.set(engine);

    // Start background GC task.
    let gc = Arc::new(GarbageCollector::new(kv_store, storage_manager));
    tokio::spawn(async move {
        gc.start().await;
    });

    tracing::info!(target: "rustfs::metadata", "New metadata engine initialized successfully.");
}
